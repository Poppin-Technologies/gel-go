// This source file is part of the Gel open source project.
//
// Copyright Gel Data Inc. and the Gel authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gel

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/geldata/gel-go/gelcfg"
	"github.com/geldata/gel-go/internal/cache"
	gelerrint "github.com/geldata/gel-go/internal/gelerr"
)

const defaultIdleConnectionTimeout = 30 * time.Second

// DefaultConcurrency is used if no other concurrency setting is found.
var DefaultConcurrency = max(4, runtime.NumCPU())

// NewPool makes a new pool.
func NewPool(dsn string, opts gelcfg.Options) (*Pool, error) { // nolint:gocritic,lll
	cfg, err := parseConnectDSNAndArgs(dsn, &opts, newCfgPaths())
	if err != nil {
		return nil, err
	}

	warningHandler := gelcfg.LogWarnings
	if opts.WarningHandler != nil {
		warningHandler = opts.WarningHandler
	}

	False := false
	p := &Pool{
		isClosed:      &False,
		isClosedMutex: &sync.RWMutex{},
		Cfg:           cfg,
		Concurrency:   int(opts.Concurrency),
		dial: newDialGate(
			opts.DialConcurrency, opts.DialBackoffMax),
		freeConns: make(chan func() *transactableConn,
			idleCapacity(opts.Concurrency)),
		potentialConnsMutext: &sync.Mutex{},
		cacheCollection: cacheCollection{
			ServerSettings:    cfg.ServerSettings,
			typeIDCache:       cache.New(1_000),
			inCodecCache:      cache.New(1_000),
			outCodecCache:     cache.New(1_000),
			capabilitiesCache: cache.New(1_000),
		},
		State: make(map[string]interface{}),
		QueryConfig: QueryConfig{
			WarningHandler: warningHandler,
			QueryOptions:   gelcfg.NewQueryOptions(),
			TxOptions:      gelcfg.NewTxOptions(),
			RetryOptions:   gelcfg.NewRetryOptions(),
			Annotations:    make(map[string]string),
		},
	}

	return p, nil
}

// Pool is a connection pool.
type Pool struct {
	isClosed      *bool
	isClosedMutex *sync.RWMutex // locks isClosed

	// A buffered channel of structs representing unconnected capacity.
	// This field remains nil until the first connection is acquired.
	potentialConns       chan struct{}
	potentialConnsMutext *sync.Mutex

	// A buffered channel of connections ready for use.
	freeConns chan func() *transactableConn

	QueryConfig QueryConfig

	Cfg             *connConfig
	cacheCollection cacheCollection
	State           map[string]interface{}

	Concurrency int

	// dial limits how many connections this pool dials at once and backs
	// off after failed dials (dialgate.go); nil when neither is configured.
	dial *dialGate
}

func (p *Pool) newConn(ctx context.Context) (*transactableConn, error) {
	conn := transactableConn{
		reconnectingConn: &reconnectingConn{
			Cfg:             p.Cfg,
			cacheCollection: p.cacheCollection,
			dial:            p.dial,
		},
	}

	if err := conn.reconnect(ctx, false); err != nil {
		return nil, err
	}

	return &conn, nil
}

// Acquire gets a connection from the pool.
func (p *Pool) Acquire(
	ctx context.Context,
) (*transactableConn, error) { // nolint:revive
	p.isClosedMutex.RLock()
	defer p.isClosedMutex.RUnlock()

	if *p.isClosed {
		return nil, gelerrint.NewInterfaceError("client closed", nil)
	}

	p.potentialConnsMutext.Lock()
	if p.potentialConns == nil {
		conn, err := p.newConn(ctx)
		if err != nil {
			p.potentialConnsMutext.Unlock()
			return nil, err
		}

		if p.Concurrency == 0 {
			// The user did not set Concurrency in provided Options.
			// See if the server sends a suggested max size.
			suggested, ok := conn.Cfg.ServerSettings.
				GetOk("suggested_pool_concurrency")
			if ok {
				p.Concurrency = suggested.(int)
			} else {
				p.Concurrency = DefaultConcurrency
			}
		}

		p.potentialConns = make(chan struct{}, p.Concurrency)
		for i := 0; i < p.Concurrency-1; i++ {
			p.potentialConns <- struct{}{}
		}

		p.potentialConnsMutext.Unlock()
		return conn, nil
	}
	p.potentialConnsMutext.Unlock()

	// force do nothing if context is expired
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("gel: %w", ctx.Err())
	default:
	}

	// force using an existing connection over connecting a new socket.
	select {
	case acquireIfNotTimedout := <-p.freeConns:
		conn := acquireIfNotTimedout()
		if conn != nil {
			return conn, nil
		}
	default:
	}

	for {
		// While the dial gate holds dials back (a dial just timed out or
		// failed temporarily), queue on a live connection first: dialling
		// into a server that cannot accept is what turned the 2026-10-31
		// slowdown into failed requests. The wait is bounded by the gate's
		// window and by ctx; a connection that frees up meanwhile is taken.
		if wait := p.dial.waitFor(); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case acquireIfNotTimedout := <-p.freeConns:
				t.Stop()
				if conn := acquireIfNotTimedout(); conn != nil {
					return conn, nil
				}
				continue
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return nil, fmt.Errorf("gel: %w", ctx.Err())
			}
		}
		select {
		case acquireIfNotTimedout := <-p.freeConns:
			conn := acquireIfNotTimedout()
			if conn != nil {
				return conn, nil
			}
			continue
		case <-p.potentialConns:
			conn, err := p.newConn(ctx)
			if err != nil {
				p.potentialConns <- struct{}{}
				return nil, err
			}
			return conn, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("gel: %w", ctx.Err())
		}
	}
}

// idleCapacity is how many released connections a pool keeps idle. Upstream
// kept ONE (freeConns had capacity 1) and closed every other connection on
// release ("we have MinConns idle"), so a shard's connection count breathed
// with its concurrency: every dip closed sockets and every rise dialled new
// ones. Under a saturated Gel frontend those re-dials time out (2026-10-31:
// hundreds of dials a minute per task, each a 3 s connect that failed), so a
// pool now keeps up to its concurrency idle and the idle timer alone trims
// connections that really sit unused. A pool whose concurrency is decided by
// the server at first connect gets DefaultConcurrency slots; a server
// suggesting more than that still sees the old close-on-release for the
// excess, counted in Stats().PoolTrims.
func idleCapacity(concurrency uint) int {
	if concurrency > 0 {
		return int(concurrency)
	}
	return DefaultConcurrency
}

// Release puts a connection back in the pool.
func (p *Pool) Release(conn *transactableConn, err error) error {
	if isClientConnectionError(err) {
		p.potentialConns <- struct{}{}
		return conn.Close()
	}

	timeout := defaultIdleConnectionTimeout
	if t, ok := conn.conn.SystemConfig.SessionIdleTimeout.Get(); ok {
		timeout = time.Duration(1_000 * t)
	}

	// 0 or less disables the idle timeout
	if timeout <= 0 {
		select {
		case p.freeConns <- func() *transactableConn { return conn }:
			return nil
		default:
			// the idle list is full: trim this connection.
			statPoolTrims.Add(1)
			p.potentialConns <- struct{}{}
			return conn.Close()
		}
	}

	cancel := make(chan struct{}, 1)
	connChan := make(chan *transactableConn, 1)

	acquireIfNotTimedout := func() *transactableConn {
		cancel <- struct{}{}
		return <-connChan
	}

	select {
	case p.freeConns <- acquireIfNotTimedout:
		go func() {
			select {
			case <-cancel:
				connChan <- conn
			case <-time.After(timeout):
				connChan <- nil
				p.potentialConns <- struct{}{}
				if e := conn.Close(); e != nil {
					log.Println("error while closing idle connection:", e)
				}
			}
		}()
	default:
		// the idle list is full: trim this connection.
		statPoolTrims.Add(1)
		p.potentialConns <- struct{}{}
		return conn.Close()
	}

	return nil
}

// Warm opens connections until the pool holds its concurrency, so a rise in
// load never has to dial into a busy server: a pool that boots with one
// connection per shard and grows on demand spends the first seconds of
// every storm dialling (2026-10-31, and the 2026-11-02 staging replay: ~500
// dials a minute per task, almost all timing out). Best effort: the first
// error stops it and is returned with the count opened so far.
func (p *Pool) Warm(ctx context.Context) (opened int, err error) {
	var held []*transactableConn
	for {
		conn, e := p.Acquire(ctx)
		if e != nil {
			err = e
			break
		}
		held = append(held, conn)
		if len(held) >= p.Concurrency {
			break
		}
	}
	for _, c := range held {
		if e := p.Release(c, nil); e != nil && err == nil {
			err = e
		}
	}
	return len(held), err
}

// EnsureConnected forces the pool to connect if it hasn't already.
func (p *Pool) EnsureConnected(ctx context.Context) error {
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}

	return p.Release(conn, nil)
}

// Close closes all connections in the pool.
// Calling close blocks until all acquired connections have been released,
// and returns an error if called more than once.
func (p *Pool) Close() error {
	p.isClosedMutex.Lock()
	defer p.isClosedMutex.Unlock()

	if *p.isClosed {
		return gelerrint.NewInterfaceError("client closed", nil)
	}
	*p.isClosed = true

	p.potentialConnsMutext.Lock()
	if p.potentialConns == nil {
		// The client never made any connections.
		p.potentialConnsMutext.Unlock()
		return nil
	}
	p.potentialConnsMutext.Unlock()

	wg := sync.WaitGroup{}
	errs := make([]error, p.Concurrency)
	for i := 0; i < p.Concurrency; i++ {
		select {
		case acquireIfNotTimedout := <-p.freeConns:
			wg.Add(1)
			go func(i int) {
				conn := acquireIfNotTimedout()
				if conn != nil {
					errs[i] = conn.Close()
				}
				wg.Done()
			}(i)
		case <-p.potentialConns:
		}
	}

	wg.Wait()
	return wrapAll(errs...)
}

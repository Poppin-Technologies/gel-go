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
	"net"
	"testing"
	"time"

	"github.com/geldata/gel-go/gelcfg"
	"github.com/geldata/gel-go/internal/buff"
	"github.com/geldata/gel-go/internal/cache"
	"github.com/geldata/gel-go/internal/soc"
)

// newIdlePipeConn builds a protocolConnection over an in-memory pipe with
// the reader loop running: the state a pooled connection rests in between
// queries. Nothing ever writes to the server end, so no message arrives,
// exactly like an idle connection.
func newIdlePipeConn(t *testing.T) *protocolConnection {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	c := &protocolConnection{
		soc:                 &autoClosingSocket{conn: client},
		acquireReaderSignal: make(chan struct{}, 1),
		readerChan:          make(chan *buff.Reader, 1),
	}
	r := buff.NewReader(make(chan *soc.Data, 2))
	if err := c.releaseReader(r); err != nil {
		t.Fatal(err)
	}
	return c
}

// acquireWithin reports whether acquireReader returned at all within limit.
func acquireWithin(
	ctx context.Context,
	c *protocolConnection,
	limit time.Duration,
) (returned bool, r *buff.Reader, err error) {
	type result struct {
		r   *buff.Reader
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := c.acquireReader(ctx)
		done <- result{r, err}
	}()
	select {
	case x := <-done:
		return true, x.r, x.err
	case <-time.After(limit):
		return false, nil, nil
	}
}

// TestAcquireReaderSurvivesCancelledWaits reproduces a production hang seen
// on 2026-10-24: acquires with an already-expired ctx (a query retried after
// its deadline) left the reader hand-off in a state no later caller could
// get past, and that caller parked forever in `chan send` on
// acquireReaderSignal.
func TestAcquireReaderSurvivesCancelledWaits(t *testing.T) {
	c := newIdlePipeConn(t)
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	// Drive the connection with dead-ctx acquires the way withRetries does
	// for a retryable error. A reader that does come back goes through
	// releaseReader, keeping the protocol honest. Stop once the stale state
	// is reached: a buffered signal nobody will consume plus a parked
	// reader with no loop goroutine left.
	for i := 0; i < 200; i++ {
		r, err := c.acquireReader(dead)
		if err == nil {
			if e := c.releaseReader(r); e != nil {
				break
			}
			continue
		}
		if c.isClosed() {
			break
		}
		time.Sleep(2 * time.Millisecond)
		if len(c.acquireReaderSignal) == 1 && len(c.readerChan) == 1 {
			break
		}
	}

	live, cancelLive := context.WithTimeout(
		context.Background(), time.Second)
	defer cancelLive()
	returned, r, err := acquireWithin(live, c, 3*time.Second)
	if !returned {
		t.Fatal("acquireReader wedged: a live caller never returned " +
			"after cancelled waits on the same connection")
	}
	if err == nil {
		_ = c.releaseReader(r)
	}
}

// TestWithRetriesStopsOnDeadContext: a query whose ctx already expired used
// to be retried twice more, with the back-off sleeps in between, because the
// ctx exit surfaces as ClientConnectionTimeoutError, which is retryable.
// Those retries are what re-entered acquireReader with a dead ctx.
func TestWithRetriesStopsOnDeadContext(t *testing.T) {
	pc := newIdlePipeConn(t)
	c := &transactableConn{reconnectingConn: &reconnectingConn{
		borrowableConn: borrowableConn{conn: pc},
		cacheCollection: cacheCollection{
			typeIDCache:       cache.New(10),
			inCodecCache:      cache.New(10),
			outCodecCache:     cache.New(10),
			capabilitiesCache: cache.New(10),
		},
	}}
	q := &query{
		cmd:    "select 1",
		method: "Query",
		cfg:    QueryConfig{RetryOptions: gelcfg.NewRetryOptions()},
	}
	// a read-only query (no capabilities) is always retryable
	c.capabilitiesCache.Put(makeKey(q), uint64(0))

	dead, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	start := time.Now()
	err := c.withRetries(dead, q, func(ctx context.Context, _ *query) error {
		calls++
		return wrapNetError(ctx.Err())
	})
	if err == nil {
		t.Fatal("expected the ctx error, got nil")
	}
	if calls != 1 {
		t.Fatalf("callback ran %d times on a dead ctx, want 1", calls)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("withRetries took %v on a dead ctx, want no back-off", d)
	}
}

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
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/geldata/gel-go/internal/buff"
)

// Why this file exists (Poppin fork, 2026-11-01). On Halloween night the
// API's requests timed out against a saturated Gel frontend, and every
// timed-out query retired its pooled connection: the flows set the socket
// read deadline to the ctx deadline, the read failed mid-reply,
// autoClosingSocket closed the socket and Pool.Release dialled a
// replacement into the same frontend, which could not accept it in time
// ("dial tcp :5656: i/o timeout", 326 pool-shard failures in one hour).
// The re-dials cost the one busy core more work exactly when it had none
// to spare, and a slow database turned into failed requests.
//
// Now a flow's read loop leaves at a MESSAGE BOUNDARY when the ctx ends
// (Reader.Next selects on the ctx-bridged done signal only between
// messages), the socket stays open, and the half-read reply is consumed in
// the background by drainThenRelease before the connection goes back to
// work. The socket deadline stays as a backstop at ctx deadline plus the
// drain budget, so a server that stops mid-message still loses the
// connection as before.

// abandonedDrainBudget bounds the background read of an abandoned reply. A
// drain that overruns it closes the socket, which is the old behaviour; the
// budget only decides how long a slow server may keep one connection out of
// the pool after its caller gave up.
var abandonedDrainBudget = 3 * time.Second

// queryInterrupted is returned when a query's ctx ended before the server's
// ReadyForCommand arrived. It unwraps to the ctx error and is deliberately
// NOT a gelerr.Error: withRetries does not retry it (the ctx is dead) and
// Pool.Release keeps the connection (its reply is being drained).
type queryInterrupted struct{ cause error }

func (e *queryInterrupted) Error() string { return "gel: " + e.cause.Error() }
func (e *queryInterrupted) Unwrap() error { return e.cause }

func newQueryInterrupted(ctx context.Context) error {
	cause := ctx.Err()
	if cause == nil {
		// Only reachable if a done signal fired without the ctx ending;
		// name a cause rather than return nil for a lost reply.
		cause = context.Canceled
	}
	return &queryInterrupted{cause: cause}
}

// interrupted reports whether err is a queryInterrupted.
func interrupted(err error) bool {
	var qi *queryInterrupted
	return errors.As(err, &qi)
}

// IsInterrupted is interrupted for the root package (gel.IsInterrupted).
func IsInterrupted(err error) bool { return interrupted(err) }

// NewInterruptedError builds a queryInterrupted around cause, for the root
// package's test helper.
func NewInterruptedError(cause error) error {
	return &queryInterrupted{cause: cause}
}

// backstopDeadline is the socket read deadline a flow sets for its query:
// the ctx deadline plus the drain budget, or none when the ctx has none.
// Ordinary ctx expiry is handled at a message boundary well before it.
func backstopDeadline(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	return deadline.Add(abandonedDrainBudget)
}

// queryDone returns the done signal a flow's read loop selects on. It fires
// when the ctx ends, so Reader.Next returns false at the next message
// boundary; the loop's ReadyForCommand case fires the same one-shot signal
// as before. stop must be called when the loop is over so the bridge
// goroutine exits. A ctx that can never end needs no bridge.
func queryDone(
	ctx context.Context,
) (done *buff.DoneReadingSignal, stop func()) {
	done = buff.NewSignal()
	if ctx == nil || ctx.Done() == nil {
		return done, func() {}
	}
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			done.Signal()
		case <-finished:
		}
	}()
	return done, func() { close(finished) }
}

// ConnectionStats are process-wide counters of the events behind
// connection churn, for the application's own metrics line.
type ConnectionStats struct {
	// Drains counts abandoned replies handed to drainThenRelease.
	Drains int64
	// DrainFailures counts drains that overran the budget or hit a
	// protocol error; each one closed its socket.
	DrainFailures int64
	// Dials counts connection attempts (reconnect.go).
	Dials int64
	// DialFailures counts connection attempts that failed.
	DialFailures int64
	// PoolTrims counts connections closed on release because the pool's idle
	// list was full (pool.go idleCapacity). Non-zero means a pool still
	// breathes connections with its concurrency.
	PoolTrims int64
}

var (
	statDrains        atomic.Int64
	statDrainFailures atomic.Int64
	statDials         atomic.Int64
	statDialFailures  atomic.Int64
	statPoolTrims     atomic.Int64
)

// Stats returns the process-wide connection counters.
func Stats() ConnectionStats {
	return ConnectionStats{
		Drains:        statDrains.Load(),
		DrainFailures: statDrainFailures.Load(),
		Dials:         statDials.Load(),
		DialFailures:  statDialFailures.Load(),
		PoolTrims:     statPoolTrims.Load(),
	}
}

// drainThenRelease consumes the rest of an interrupted command's reply in
// the background, every message up to and including ReadyForCommand, under
// a socket deadline of abandonedDrainBudget, then hands the reader to the
// idle loop (releaseReader) so the connection serves the next query. State
// descriptions are decoded so the connection's state codec stays current;
// data, completion and error messages belong to the abandoned query and
// are discarded. A drain that fails or overruns closes the socket and parks
// the reader with the error: the next acquirer sees
// ClientConnectionClosedError and the pool replaces the connection as it
// always did.
//
// While the drain runs the connection may already sit in the pool's free
// list. An acquirer sends its hand-off signal and waits on readerChan; the
// drain never consumes that signal, so the acquirer gets the reader the
// moment the idle loop starts, or fails fast if the drain closed the
// socket. Its wait is bounded by its own ctx (acquireReader retires the
// connection if that runs out first), so a long drain costs at most one
// dial, where a torn-down connection always cost one.
func (c *protocolConnection) drainThenRelease(r *buff.Reader) {
	statDrains.Add(1)
	go func() {
		deadline := time.Now().Add(abandonedDrainBudget)
		if err := c.soc.SetDeadline(deadline); err != nil {
			c.parkFailedDrain(r, err)
			return
		}
		done := buff.NewSignal()
	drain:
		for r.Next(done.Chan) {
			switch Message(r.MsgType) {
			case ReadyForCommand:
				decodeReadyForCommandMsg(r)
				done.Signal()
			case StateDataDescription:
				if e := c.decodeStateDataDescription(r); e != nil {
					r.Err = e
					break drain
				}
			case Data, CommandComplete, CommandDataDescription, ErrorResponse:
				r.DiscardMessage()
			default:
				if e := c.fallThrough(r); e != nil {
					r.Err = e
					break drain
				}
			}
		}
		if r.Err != nil {
			c.parkFailedDrain(r, r.Err)
			return
		}
		if err := c.soc.SetDeadline(time.Time{}); err != nil {
			c.parkFailedDrain(r, err)
			return
		}
		if err := c.releaseReader(r); err != nil {
			c.parkFailedDrain(r, err)
		}
	}()
}

// parkFailedDrain closes the socket and parks the reader carrying err, so
// the next acquirer fails fast with ClientConnectionClosedError and the
// pool replaces the connection.
func (c *protocolConnection) parkFailedDrain(r *buff.Reader, err error) {
	statDrainFailures.Add(1)
	if r.Err == nil {
		r.Err = err
	}
	if e := c.soc.Close(); e != nil {
		log.Println("error closing socket after a failed drain:", e)
	}
	c.readerChan <- r
}

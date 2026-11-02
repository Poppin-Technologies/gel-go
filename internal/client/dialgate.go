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
	"sync"
	"time"

	"github.com/geldata/gel-go/gelerr"
	gelerrint "github.com/geldata/gel-go/internal/gelerr"
)

// dialGate limits how hard one pool knocks on a server that is not
// answering (Poppin fork, 2026-11-01). A saturated Gel frontend cannot
// accept new connections in time, and every dial it fails to answer costs
// it the accept work anyway; a pool that re-dials sixty-four connections
// at once, three times over, turns a slow server into a dead one. The gate
// caps concurrent dials and, after a dial times out or fails temporarily,
// holds the next one back for a doubling window until a dial succeeds.
// Healthy pooled connections are never affected: only a connection that
// needs to be (re)established passes through here.
type dialGate struct {
	slots      chan struct{} // nil: no concurrency cap
	maxBackoff time.Duration // <= 0: no backoff

	mu        sync.Mutex
	backoff   time.Duration
	notBefore time.Time
}

// dialBackoffFirst is the window after the first failed dial; it doubles
// per failure up to maxBackoff and clears on a success.
const dialBackoffFirst = 100 * time.Millisecond

// newDialGate returns nil when neither limit is set, so an unconfigured
// pool pays one nil check and nothing else.
func newDialGate(concurrency uint, maxBackoff time.Duration) *dialGate {
	if concurrency == 0 && maxBackoff <= 0 {
		return nil
	}
	g := &dialGate{maxBackoff: maxBackoff}
	if concurrency > 0 {
		g.slots = make(chan struct{}, concurrency)
	}
	return g
}

// acquire waits for the backoff window to pass and for a dial slot. When
// ctx ends first it returns a temporary connection error wrapping the ctx
// error, and no slot is held (do not call release).
func (g *dialGate) acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if wait := g.waitFor(); wait > 0 {
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return gelerrint.NewClientConnectionFailedTemporarilyError(
				"dial held back by backoff", ctx.Err())
		}
	}
	if g.slots != nil {
		select {
		case g.slots <- struct{}{}:
		case <-ctx.Done():
			return gelerrint.NewClientConnectionFailedTemporarilyError(
				"waiting for a dial slot", ctx.Err())
		}
	}
	return nil
}

// waitFor is how long the backoff still holds dials back (0 for a nil gate).
func (g *dialGate) waitFor() time.Duration {
	if g == nil || g.maxBackoff <= 0 {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Until(g.notBefore)
}

// release frees the dial slot taken by acquire and feeds the dial's outcome
// into the backoff: a success clears it, a timeout or temporary failure
// doubles it (dialBackoffFirst up to maxBackoff). Any other error, such as
// a bad password or a ctx the caller cancelled, leaves it alone.
func (g *dialGate) release(err error) {
	if g == nil {
		return
	}
	if g.slots != nil {
		<-g.slots
	}
	if g.maxBackoff <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case err == nil:
		// One success opens the door again but only halves the window, so
		// a server that accepts every other dial does not reset the brake.
		g.backoff /= 2
		if g.backoff < dialBackoffFirst {
			g.backoff = 0
		}
		g.notBefore = time.Time{}
	case isDialPressure(err):
		if g.backoff == 0 {
			g.backoff = dialBackoffFirst
		} else {
			g.backoff *= 2
		}
		if g.backoff > g.maxBackoff {
			g.backoff = g.maxBackoff
		}
		g.notBefore = time.Now().Add(g.backoff)
	}
}

// isDialPressure reports whether a dial failed the way a server under
// pressure fails: it did not answer in time, or refused. A ctx the caller
// cancelled is not pressure.
func isDialPressure(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var edbErr gelerr.Error
	if !errors.As(err, &edbErr) {
		return false
	}
	return edbErr.Category(gelerr.ClientConnectionTimeoutError) ||
		edbErr.Category(gelerr.ClientConnectionFailedTemporarilyError)
}

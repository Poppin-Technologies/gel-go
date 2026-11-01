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
	"testing"
	"time"

	gelerrint "github.com/geldata/gel-go/internal/gelerr"
)

func timeoutErr() error {
	return gelerrint.NewClientConnectionTimeoutError("", errors.New("dial"))
}

// TestNilDialGateIsFree: an unconfigured pool has a nil gate and nothing
// to pay for.
func TestNilDialGateIsFree(t *testing.T) {
	var g *dialGate
	if g != newDialGate(0, 0) {
		t.Fatal("no limits must produce a nil gate")
	}
	if err := g.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.release(nil)
	g.release(timeoutErr())
}

// TestDialGateCapsConcurrency: a third dial waits for a slot and gives up
// with its ctx; releasing one slot lets it through.
func TestDialGateCapsConcurrency(t *testing.T) {
	g := newDialGate(2, 0)
	ctx := context.Background()
	if err := g.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.acquire(ctx); err != nil {
		t.Fatal(err)
	}

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err := g.acquire(short)
	if err == nil {
		t.Fatal("a third dial got a slot past the cap of two")
	}
	if !isClientConnectionError(err) {
		t.Fatalf("slot wait surfaced as %T, want a client connection error",
			err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("slot wait error does not unwrap to the ctx error")
	}

	g.release(nil)
	if err := g.acquire(ctx); err != nil {
		t.Fatalf("a slot was released but the next dial got %v", err)
	}
}

// TestDialGateBacksOffAfterPressure: timeouts open a doubling window,
// a success closes it, and a caller's own cancel is not pressure.
func TestDialGateBacksOffAfterPressure(t *testing.T) {
	g := newDialGate(0, time.Second)
	if w := g.waitFor(); w > 0 {
		t.Fatalf("fresh gate waits %v", w)
	}

	g.release(timeoutErr())
	if w := g.waitFor(); w < 50*time.Millisecond || w > 100*time.Millisecond {
		t.Fatalf("after one timeout the window is %v, want about 100 ms", w)
	}
	start := time.Now()
	if err := g.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("acquire did not wait out the window (%v)", d)
	}

	g.release(timeoutErr())
	w := g.waitFor()
	if w <= 100*time.Millisecond || w > 200*time.Millisecond {
		t.Fatalf("after two timeouts the window is %v, want about 200 ms", w)
	}

	g.release(nil)
	if w := g.waitFor(); w > 0 {
		t.Fatalf("a success left a window of %v", w)
	}

	cancelled := gelerrint.NewClientConnectionTimeoutError("",
		context.Canceled)
	g.release(cancelled)
	if w := g.waitFor(); w > 0 {
		t.Fatalf("a caller's cancel opened a window of %v", w)
	}
}

// TestDialGateBackoffCaps: the window never grows past maxBackoff.
func TestDialGateBackoffCaps(t *testing.T) {
	g := newDialGate(0, 250*time.Millisecond)
	for i := 0; i < 6; i++ {
		g.release(timeoutErr())
	}
	if w := g.waitFor(); w > 250*time.Millisecond {
		t.Fatalf("window %v grew past the cap", w)
	}
}

// TestDialGateWaitHonoursCtx: a dial that cannot wait out the window fails
// at once with a temporary error instead of parking the request.
func TestDialGateWaitHonoursCtx(t *testing.T) {
	g := newDialGate(0, time.Second)
	for i := 0; i < 4; i++ {
		g.release(timeoutErr())
	}
	ctx, cancel := context.WithTimeout(context.Background(),
		30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := g.acquire(ctx)
	if err == nil {
		t.Fatal("acquire returned nil inside the backoff window")
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("acquire waited %v past its ctx", d)
	}
	if !isClientConnectionError(err) {
		t.Fatalf("backoff wait surfaced as %T", err)
	}
}

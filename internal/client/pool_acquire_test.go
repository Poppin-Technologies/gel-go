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
	"sync"
	"testing"
	"time"
)

func heldBackPool(t *testing.T, idle int) *Pool {
	t.Helper()
	closed := false
	p := &Pool{
		isClosed:             &closed,
		isClosedMutex:        &sync.RWMutex{},
		potentialConns:       make(chan struct{}, 1),
		potentialConnsMutext: &sync.Mutex{},
		freeConns:            make(chan func() *transactableConn, idle),
		Concurrency:          idle,
		dial:                 newDialGate(0, 2*time.Second),
	}
	p.potentialConns <- struct{}{} // a dial slot is open
	for i := 0; i < 4; i++ {
		p.dial.release(timeoutErr()) // four timeouts: dials held for ~800 ms
	}
	if w := p.dial.waitFor(); w < 500*time.Millisecond {
		t.Fatalf("gate window %v, want the backoff in force", w)
	}
	return p
}

// TestAcquirePrefersFreeConnWhileDialsAreHeldBack: with the gate in backoff
// and a dial slot open, Acquire must take a connection that frees up instead
// of dialling, as soon as it appears, and leave the dial slot alone.
func TestAcquirePrefersFreeConnWhileDialsAreHeldBack(t *testing.T) {
	p := heldBackPool(t, 4)
	want := &transactableConn{}
	go func() {
		time.Sleep(50 * time.Millisecond)
		p.freeConns <- func() *transactableConn { return want }
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	got, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("Acquire did not hand out the connection that freed up")
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("Acquire waited %v for a connection free after 50 ms", d)
	}
	if len(p.potentialConns) != 1 {
		t.Fatal("Acquire spent the dial slot although a connection freed up")
	}
}

// TestAcquireHonoursCtxWhileDialsAreHeldBack: a caller whose ctx ends inside
// the gate window gets the ctx error at once, with the dial slot untouched.
func TestAcquireHonoursCtxWhileDialsAreHeldBack(t *testing.T) {
	p := heldBackPool(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(),
		60*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.Acquire(ctx); err == nil {
		t.Fatal("Acquire returned a connection it could not have had")
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("Acquire outlived its ctx by %v", d)
	}
	if len(p.potentialConns) != 1 {
		t.Fatal("the dial slot was consumed by a caller that gave up")
	}
}

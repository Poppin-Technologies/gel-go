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
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/geldata/gel-go/internal/buff"
	"github.com/geldata/gel-go/internal/soc"
)

// newDrainablePipeConn builds a protocolConnection over an in-memory pipe
// with the socket read loop running and the reader ACQUIRED: the state a
// connection is in the moment a query's ctx ends mid-reply, before
// drainThenRelease takes over. The returned server end stands in for Gel
// and still owes the rest of the reply.
func newDrainablePipeConn(
	t *testing.T,
) (*protocolConnection, net.Conn, *buff.Reader) {
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
		protocolVersion:     protocolVersion2p0,
	}
	toBeDeserialized := make(chan *soc.Data, 2)
	go soc.Read(c.soc, soc.NewMemPool(4, 256*1024), toBeDeserialized)
	return c, server, buff.NewReader(toBeDeserialized)
}

// frame encodes one protocol message: type byte, uint32 length that counts
// itself, payload.
func frame(msg Message, payload []byte) []byte {
	b := make([]byte, 0, 5+len(payload))
	b = append(b, byte(msg))
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)+4))
	return append(b, payload...)
}

var (
	// a Data message with one element of one byte; the drain discards it
	drainTestData = frame(Data, []byte{0, 1, 0, 0, 0, 1, 0x2a})
	// a CommandComplete; its payload is irrelevant to a drain
	drainTestComplete = frame(CommandComplete,
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 'O', 'K'})
	// ReadyForCommand: no annotations, transaction state idle
	drainTestReady = frame(ReadyForCommand, []byte{0, 0, 'I'})
)

// TestDrainThenReleaseKeepsConnection: the caller gave up mid-reply; the
// server finishes the reply within the budget; the connection must come
// back to the idle loop open and hand its reader to the next acquirer.
func TestDrainThenReleaseKeepsConnection(t *testing.T) {
	c, server, r := newDrainablePipeConn(t)
	before := Stats()

	go func() {
		time.Sleep(50 * time.Millisecond)
		for _, m := range [][]byte{
			drainTestData, drainTestComplete, drainTestReady,
		} {
			if _, err := server.Write(m); err != nil {
				return
			}
		}
	}()

	c.drainThenRelease(r)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	returned, r2, err := acquireWithin(ctx, c, 3*time.Second)
	if !returned {
		t.Fatal("acquireReader never returned after a drain")
	}
	if err != nil {
		t.Fatalf("acquireReader after a drain: %v", err)
	}
	if c.isClosed() {
		t.Fatal("the drain closed a socket whose reply completed in time")
	}
	if e := c.releaseReader(r2); e != nil {
		t.Fatal(e)
	}
	after := Stats()
	if after.Drains-before.Drains != 1 {
		t.Fatalf("Drains counted %d, want 1", after.Drains-before.Drains)
	}
	if after.DrainFailures != before.DrainFailures {
		t.Fatal("a successful drain was counted as a failure")
	}
}

// TestDrainOverrunClosesConnection: a server that never finishes the reply
// must not keep the connection hostage; past the budget the socket closes
// and the next acquirer fails fast with a connection error, which is what
// makes the pool replace the connection.
func TestDrainOverrunClosesConnection(t *testing.T) {
	old := abandonedDrainBudget
	abandonedDrainBudget = 150 * time.Millisecond
	t.Cleanup(func() { abandonedDrainBudget = old })

	c, _, r := newDrainablePipeConn(t)
	before := Stats()
	c.drainThenRelease(r)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	returned, _, err := acquireWithin(ctx, c, 3*time.Second)
	if !returned {
		t.Fatal("acquireReader never returned after an overrun drain")
	}
	if err == nil {
		t.Fatal("acquireReader returned a reader on a connection whose " +
			"drain overran")
	}
	if !isClientConnectionError(err) {
		t.Fatalf("overrun surfaced as %T, want a client connection error "+
			"so the pool replaces the connection", err)
	}
	if !c.isClosed() {
		t.Fatal("the overrun drain left the socket open")
	}
	after := Stats()
	if after.DrainFailures-before.DrainFailures != 1 {
		t.Fatalf("DrainFailures counted %d, want 1",
			after.DrainFailures-before.DrainFailures)
	}
}

// TestQueryDoneBridgesCtx: the done signal fires when the ctx ends, and a
// ctx that can never end gets no bridge goroutine and no signal.
func TestQueryDoneBridgesCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done, stop := queryDone(ctx)
	defer stop()
	cancel()
	select {
	case <-done.Chan:
	case <-time.After(time.Second):
		t.Fatal("done never fired after the ctx was cancelled")
	}

	plain, stopPlain := queryDone(context.Background())
	defer stopPlain()
	select {
	case <-plain.Chan:
		t.Fatal("a background ctx fired the done signal")
	case <-time.After(20 * time.Millisecond):
	}
	plain.Signal() // the loop's own ReadyForCommand case still works
	select {
	case <-plain.Chan:
	default:
		t.Fatal("Signal did not deliver on an unbridged done")
	}
}

// TestBackstopDeadline: a deadline moves out by the drain budget; no
// deadline stays no deadline.
func TestBackstopDeadline(t *testing.T) {
	if !backstopDeadline(context.Background()).IsZero() {
		t.Fatal("a ctx without a deadline got a socket deadline")
	}
	at := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), at)
	defer cancel()
	got := backstopDeadline(ctx)
	if want := at.Add(abandonedDrainBudget); !got.Equal(want) {
		t.Fatalf("backstop %v, want %v", got, want)
	}
}

// TestInterruptedIsNotAConnectionError: the pool must keep the connection
// (its reply is draining) and withRetries must not retry a dead ctx.
func TestInterruptedIsNotAConnectionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := newQueryInterrupted(ctx)
	if !interrupted(err) {
		t.Fatal("interrupted() does not recognise its own error")
	}
	if isClientConnectionError(err) {
		t.Fatal("an interrupted query reads as a connection error")
	}
	if !errorsIsCanceled(err) {
		t.Fatal("an interrupted query does not unwrap to the ctx error")
	}
}

func errorsIsCanceled(err error) bool {
	for err != nil {
		if err == context.Canceled {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

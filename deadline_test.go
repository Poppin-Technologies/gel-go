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

	gelint "github.com/geldata/gel-go/internal/client"
)

// TestQueryDeadlineKeepsConnection is the end-to-end proof of the drain
// (internal/client/drain.go): a query that outlives its ctx must fail with
// the ctx error, and the SAME connection must serve the next query without
// a re-dial. Before the fork patch the timed-out read closed the socket and
// the pool dialled a replacement.
func TestQueryDeadlineKeepsConnection(t *testing.T) {
	o := opts
	o.Concurrency = 1 // one connection: any re-dial shows up in Stats
	c, err := CreateClient(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var warm int64
	if e := c.QuerySingle(ctx, "SELECT 1", &warm); e != nil {
		t.Fatal(e)
	}
	before := gelint.Stats()

	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	var slept bool
	err = c.QuerySingle(short, "SELECT sys::_sleep(<int64>2)", &slept)
	if err == nil {
		t.Fatal("a 2 s sleep finished inside a 300 ms deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out query returned %T %v, want the ctx error",
			err, err)
	}

	// The reply is still streaming on the connection; the next query waits
	// for the drain (bounded by the sleep's remaining ~1.7 s) and then runs
	// on the same socket.
	start := time.Now()
	var one int64
	if err := c.QuerySingle(ctx, "SELECT 1", &one); err != nil {
		t.Fatalf("query after a timed-out query failed: %v", err)
	}
	if one != 1 {
		t.Fatalf("got %d, want 1", one)
	}
	t.Logf("follow-up query took %v", time.Since(start))

	after := gelint.Stats()
	if after.Dials != before.Dials {
		t.Fatalf("the timed-out query cost %d re-dial(s); the connection "+
			"was torn down instead of drained", after.Dials-before.Dials)
	}
	if after.Drains-before.Drains != 1 {
		t.Fatalf("Drains moved by %d, want 1", after.Drains-before.Drains)
	}
	if after.DrainFailures != before.DrainFailures {
		t.Fatal("the drain was counted as a failure")
	}
}

// TestQueryDeadlineDrainOverrunReconnects: when the server does not finish
// the abandoned reply inside the drain budget the socket is closed, and the
// pool dials exactly one replacement for the next query, as before.
func TestQueryDeadlineDrainOverrunReconnects(t *testing.T) {
	o := opts
	o.Concurrency = 1
	c, err := CreateClient(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var warm int64
	if e := c.QuerySingle(ctx, "SELECT 1", &warm); e != nil {
		t.Fatal(e)
	}
	before := gelint.Stats()

	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	var slept bool
	// 5 s is past the 3 s drain budget: the drain must give up and close.
	err = c.QuerySingle(short, "SELECT sys::_sleep(<int64>5)", &slept)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the ctx error", err)
	}

	var one int64
	if err := c.QuerySingle(ctx, "SELECT 1", &one); err != nil {
		t.Fatalf("query after an overrun drain failed: %v", err)
	}
	after := gelint.Stats()
	if after.DrainFailures-before.DrainFailures != 1 {
		t.Fatalf("DrainFailures moved by %d, want 1",
			after.DrainFailures-before.DrainFailures)
	}
	if after.Dials-before.Dials != 1 {
		t.Fatalf("Dials moved by %d, want exactly 1 replacement",
			after.Dials-before.Dials)
	}
}

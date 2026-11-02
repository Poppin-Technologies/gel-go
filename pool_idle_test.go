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
)

// burst runs n one-second queries at once on c and waits for them all, so
// the pool must hold n connections at the same time.
func burst(t *testing.T, c *Client, n int) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var slept bool
			errs <- c.QuerySingle(context.Background(),
				"SELECT sys::_sleep(<int64>1)", &slept)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestPoolKeepsReleasedConnectionsIdle: a second burst of the same size must
// not dial again. Upstream kept one idle connection per pool and closed the
// rest on release, so every burst after the first re-dialled n-1 sockets.
func TestPoolKeepsReleasedConnectionsIdle(t *testing.T) {
	o := opts
	o.Concurrency = 4
	c, err := CreateClient(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	burst(t, c, 4)
	warm := Stats()
	burst(t, c, 4)
	burst(t, c, 4)
	after := Stats()

	if d := after.Dials - warm.Dials; d != 0 {
		t.Fatalf("two more bursts cost %d re-dial(s); connections were "+
			"closed on release", d)
	}
	if tr := after.PoolTrims - warm.PoolTrims; tr != 0 {
		t.Fatalf("pool trimmed %d connection(s) with the idle list sized to "+
			"the concurrency", tr)
	}
}

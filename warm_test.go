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
	"testing"
)

// TestWarmFillsThePool: Warm opens the full concurrency up front, and a burst
// of that size afterwards dials nothing.
func TestWarmFillsThePool(t *testing.T) {
	o := opts
	o.Concurrency = 4
	c, err := CreateClient(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	before := Stats()
	n, err := c.Warm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("Warm holds %d connections, want 4", n)
	}
	warmed := Stats()
	if d := warmed.Dials - before.Dials; d != 4 {
		t.Fatalf("Warm dialled %d times, want 4", d)
	}
	burst(t, c, 4)
	after := Stats()
	if d := after.Dials - warmed.Dials; d != 0 {
		t.Fatalf("a burst after Warm cost %d dial(s)", d)
	}
}

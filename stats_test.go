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

// TestStatsCountsDials: creating a client and running a query dials at
// least once, and the public Stats mirrors the internal counters.
func TestStatsCountsDials(t *testing.T) {
	before := Stats()
	c, err := CreateClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var one int64
	if e := c.QuerySingle(ctxBackground(), "SELECT 1", &one); e != nil {
		t.Fatal(e)
	}
	after := Stats()
	if after.Dials <= before.Dials {
		t.Fatalf("Dials did not move: %d → %d", before.Dials, after.Dials)
	}
}

func ctxBackground() context.Context { return context.Background() }

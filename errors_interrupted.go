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

	gelint "github.com/geldata/gel-go/internal/client"
)

// IsInterrupted reports whether err came from a query whose ctx ended after
// the query reached the server but before its reply was complete. Such an
// error unwraps to the ctx error like a pool-acquire timeout does, yet it
// means the opposite about the pool: a connection WAS available and the
// server was slow; the connection is being drained and kept. Callers that
// classify "pool starved" by a plain ctx error must exclude this case.
func IsInterrupted(err error) bool { return gelint.IsInterrupted(err) }

// NewInterruptedError builds the error a query interrupted by ctx returns,
// for tests of error classification. cause is normally ctx.Err().
func NewInterruptedError(cause error) error {
	if cause == nil {
		cause = context.Canceled
	}
	return gelint.NewInterruptedError(cause)
}

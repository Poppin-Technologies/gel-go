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
	"fmt"
	"testing"
)

func TestIsInterrupted(t *testing.T) {
	err := NewInterruptedError(context.DeadlineExceeded)
	if !IsInterrupted(err) {
		t.Fatal("IsInterrupted rejects its own error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("an interrupted error does not unwrap to its cause")
	}
	wrapped := fmt.Errorf("handler: %w", err)
	if !IsInterrupted(wrapped) {
		t.Fatal("IsInterrupted does not see through fmt.Errorf wrapping")
	}
	if IsInterrupted(context.DeadlineExceeded) {
		t.Fatal("a bare ctx error (pool-acquire timeout) reads as interrupted")
	}
	if IsInterrupted(fmt.Errorf("gel: %w", context.DeadlineExceeded)) {
		t.Fatal("the pool's own ctx error shape reads as interrupted")
	}
	if !IsInterrupted(NewInterruptedError(nil)) {
		t.Fatal("a nil cause must still build an interrupted error")
	}
}

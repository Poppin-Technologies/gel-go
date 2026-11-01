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

import gelint "github.com/geldata/gel-go/internal/client"

// ConnectionStats are process-wide counters of the events behind
// connection churn: abandoned replies drained in the background (and the
// drains that overran and closed their socket), and connections dialled
// (and the dials that failed). Applications log them next to their own
// pool metrics; a rising DialFailures with flat Dials means the server is
// not accepting connections in time.
type ConnectionStats = gelint.ConnectionStats

// Stats returns the process-wide connection counters.
func Stats() ConnectionStats { return gelint.Stats() }

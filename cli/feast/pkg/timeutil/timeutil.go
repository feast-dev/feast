// Copyright 2018 The Feast Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timeutil

import (
	"fmt"
	"math"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

func DurationUntilNowInHumanFormat(timestamp timestamp.Timestamp) string {
	timeSinceCreation := float64(time.Now().Unix() - timestamp.GetSeconds())
	if days := math.Floor(timeSinceCreation / float64(8.64e+4)); days > 0 {
		return fmt.Sprintf("%dd", int(days))
	} else if hours := math.Floor(timeSinceCreation / float64(3.6e+3)); hours > 0 {
		return fmt.Sprintf("%dh", int(hours))
	}
	return fmt.Sprintf("%dm", int(math.Floor(timeSinceCreation/float64(60))))
}

func FormatToRFC3339(ts timestamp.Timestamp) string {
	t := time.Unix(ts.GetSeconds(), int64(ts.GetNanos()))
	return t.Format(time.RFC3339)
}

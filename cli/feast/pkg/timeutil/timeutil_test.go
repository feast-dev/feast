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
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

func TestDurationUntilNowInHumanFormat(t *testing.T) {
	type args struct {
		createdTimestamp timestamp.Timestamp
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{"59 second before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-1 * time.Duration(time.Second)).Unix()}}, "0m"},
		{"1 minute before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-1 * time.Duration(time.Minute)).Unix()}}, "1m"},
		{"30 minutes before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-30 * time.Duration(time.Minute)).Unix()}}, "30m"},
		{"1 hour before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-1 * time.Duration(time.Hour)).Unix()}}, "1h"},
		{"2 hours before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-2 * time.Duration(time.Hour)).Unix()}}, "2h"},
		{"1 day before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-24 * time.Duration(time.Hour)).Unix()}}, "1d"},
		{"2 days before now", args{timestamp.Timestamp{Seconds: time.Now().Add(-48 * time.Duration(time.Hour)).Unix()}}, "2d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DurationUntilNowInHumanFormat(tt.args.createdTimestamp); got != tt.want {
				t.Errorf("DurationUntilNowInHumanFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

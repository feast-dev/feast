/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"reflect"
	"testing"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
)

func TestWithCommandPrefix(t *testing.T) {
	cmd := []string{"feast", "serve", "-h", "0.0.0.0", "-p", "6566"}
	cases := []struct {
		name string
		cfg  *feastdevv1.ServerConfigs
		want []string
	}{
		{"nil config", nil, cmd},
		{"no prefix", &feastdevv1.ServerConfigs{}, cmd},
		{"otel wrapper", &feastdevv1.ServerConfigs{CommandPrefix: []string{"opentelemetry-instrument"}},
			[]string{"opentelemetry-instrument", "feast", "serve", "-h", "0.0.0.0", "-p", "6566"}},
		{"multi-token wrapper", &feastdevv1.ServerConfigs{CommandPrefix: []string{"wrapper", "--flag", "--"}},
			[]string{"wrapper", "--flag", "--", "feast", "serve", "-h", "0.0.0.0", "-p", "6566"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withCommandPrefix(cmd, tc.cfg); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

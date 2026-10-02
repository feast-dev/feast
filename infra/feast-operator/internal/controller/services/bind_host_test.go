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
	"testing"

	"k8s.io/utils/ptr"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
)

func TestWithBindHost(t *testing.T) {
	dual := &feastdevv1.ServerConfigs{DualStack: ptr.To(true)}
	cases := []struct {
		name      string
		feastType FeastServiceType
		args      []string
		cfg       *feastdevv1.ServerConfigs
		want      []string
	}{
		{"unset keeps ipv4", OnlineFeastType, []string{"serve", "-h", hostAllIPv4}, &feastdevv1.ServerConfigs{}, []string{"serve", "-h", hostAllIPv4}},
		{"nil config keeps ipv4", UIFeastType, []string{"ui", "-h", hostAllIPv4}, nil, []string{"ui", "-h", hostAllIPv4}},
		{"false keeps ipv4", OfflineFeastType, []string{"serve_offline", "-h", hostAllIPv4}, &feastdevv1.ServerConfigs{DualStack: ptr.To(false)}, []string{"serve_offline", "-h", hostAllIPv4}},
		{"online bracketed", OnlineFeastType, []string{"serve", "--metrics", "-h", hostAllIPv4}, dual, []string{"serve", "--metrics", "-h", "[::]"}},
		{"offline bracketed", OfflineFeastType, []string{"serve_offline", "-h", hostAllIPv4}, dual, []string{"serve_offline", "-h", "[::]"}},
		{"ui bare", UIFeastType, []string{"ui", "-h", hostAllIPv4}, dual, []string{"ui", "-h", "::"}},
		{"-h as first arg is still replaced", LineageFeastType, []string{"-h", hostAllIPv4}, dual, []string{"-h", "::"}},
		{"-h with no following value does not panic or get touched", OnlineFeastType, []string{"serve", "-h"}, dual, []string{"serve", "-h"}},
		{"lineage bare", LineageFeastType, []string{"serve_lineage", "-h", hostAllIPv4}, dual, []string{"serve_lineage", "-h", "::"}},
		{"registry has no host flag", RegistryFeastType, []string{"serve_registry"}, dual, []string{"serve_registry"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := append([]string{}, tc.args...)
			got := withBindHost(tc.feastType, in, tc.cfg)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
			for i := range in {
				if in[i] != tc.args[i] {
					t.Fatalf("input mutated: %v", in)
				}
			}
		})
	}
}

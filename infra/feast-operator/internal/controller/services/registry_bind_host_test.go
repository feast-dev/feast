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

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"k8s.io/utils/ptr"
)

func registryFeastServices(restEnabled bool, dualStack *bool) *FeastServices {
	fs := &feastdevv1.FeatureStore{}
	fs.Status.Applied.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					ServerConfigs: feastdevv1.ServerConfigs{DualStack: dualStack},
					GRPC:          ptr.To(true),
					RestAPI:       ptr.To(restEnabled),
				},
			},
		},
	}
	return &FeastServices{Handler: handler.FeastHandler{FeatureStore: fs}}
}

func TestRegistryContainerCommandHostFlag(t *testing.T) {
	cases := []struct {
		name        string
		restEnabled bool
		dualStack   *bool
		wantHost    string // empty: no -h expected
	}{
		{"unset keeps ipv4", true, nil, hostAllIPv4},
		{"explicit false keeps ipv4", true, ptr.To(false), hostAllIPv4},
		{"explicit true binds bare ipv6 wildcard", true, ptr.To(true), hostAllIPv6},
		{"rest disabled never renders -h", false, ptr.To(true), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			feast := registryFeastServices(tc.restEnabled, tc.dualStack)
			args := feast.getContainerCommand(RegistryFeastType)

			gotHost := ""
			for i, a := range args {
				if a == "-h" && i+1 < len(args) {
					gotHost = args[i+1]
				}
			}
			if gotHost != tc.wantHost {
				t.Fatalf("got args %v, want -h %q", args, tc.wantHost)
			}
		})
	}
}

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
	"os"
	"testing"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"k8s.io/utils/ptr"
)

func TestGetInitContainerImage(t *testing.T) {
	customInit := "quay.io/org/feast-init:custom"
	envImage := "quay.io/org/feast-env:test"

	t.Run("uses initImage and ignores differing server images", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreServices{
			InitImage: ptr.To(customInit),
			OfflineStore: &feastdevv1.OfflineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr.To("quay.io/org/offline:v1"),
						},
					},
				},
			},
			OnlineStore: &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr.To("quay.io/org/online:v1"),
						},
					},
				},
			},
		})
		if got != customInit {
			t.Fatalf("got %q, want %q (must not inherit server images)", got, customInit)
		}
	})

	t.Run("falls back to RELATED_IMAGE_FEATURE_SERVER", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreServices{})
		if got != envImage {
			t.Fatalf("got %q, want %q", got, envImage)
		}
	})

	t.Run("falls back to DefaultImage", func(t *testing.T) {
		os.Unsetenv(feastServerImageVar)
		got := getInitContainerImage(nil)
		if got != DefaultImage {
			t.Fatalf("got %q, want %q", got, DefaultImage)
		}
	})

	t.Run("ignores empty initImage", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreServices{
			InitImage: ptr.To(""),
		})
		if got != envImage {
			t.Fatalf("got %q, want %q", got, envImage)
		}
	})
}

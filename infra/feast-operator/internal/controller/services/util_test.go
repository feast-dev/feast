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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"
)

var _ = Describe("ApplyDefaultsToStatus", func() {
	It("deploys the online store with defaults when it is not declared", func() {
		cr := &feastdevv1.FeatureStore{
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "test_project",
				Services:     &feastdevv1.FeatureStoreServices{},
			},
		}

		ApplyDefaultsToStatus(cr)

		online := cr.Status.Applied.Services.OnlineStore
		Expect(online).ToNot(BeNil())
		Expect(online.Disabled).To(BeFalse())
		Expect(online.Persistence).ToNot(BeNil())
		Expect(online.Server).ToNot(BeNil())
	})

	It("applies online store defaults when it is declared", func() {
		cr := &feastdevv1.FeatureStore{
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "test_project",
				Services: &feastdevv1.FeatureStoreServices{
					OnlineStore: &feastdevv1.OnlineStore{},
				},
			},
		}

		ApplyDefaultsToStatus(cr)

		online := cr.Status.Applied.Services.OnlineStore
		Expect(online).ToNot(BeNil())
		Expect(online.Persistence).ToNot(BeNil())
		Expect(online.Server).ToNot(BeNil())
	})

	// #6586: disabling the online store opts out of its persistence and serving
	// pod, letting a registry-only or offline-only ViewerStore skip it while
	// leaving the default-on behavior unchanged for everyone else.
	It("does not apply persistence or server defaults when the online store is disabled", func() {
		cr := &feastdevv1.FeatureStore{
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "test_project",
				Services: &feastdevv1.FeatureStoreServices{
					OnlineStore: &feastdevv1.OnlineStore{Disabled: true},
				},
			},
		}

		ApplyDefaultsToStatus(cr)

		online := cr.Status.Applied.Services.OnlineStore
		Expect(online).ToNot(BeNil())
		Expect(online.Disabled).To(BeTrue())
		Expect(online.Persistence).To(BeNil())
		Expect(online.Server).To(BeNil())
	})
})

func TestGetInitContainerImage(t *testing.T) {
	customInit := "quay.io/org/feast-init:custom"
	packagedImage := "quay.io/org/feast-packaged:test"
	envImage := "quay.io/org/feast-env:test"

	t.Run("uses initImage ahead of packaged and server images", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreSpec{
			FeastProjectDir: &feastdevv1.FeastProjectDir{
				Packaged: &feastdevv1.FeastPackagedOptions{Image: packagedImage},
			},
			Services: &feastdevv1.FeatureStoreServices{
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
			},
		})
		if got != customInit {
			t.Fatalf("got %q, want %q (must not inherit server images)", got, customInit)
		}
	})

	t.Run("uses packaged image ahead of RELATED_IMAGE_FEATURE_SERVER", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreSpec{
			FeastProjectDir: &feastdevv1.FeastProjectDir{
				Packaged: &feastdevv1.FeastPackagedOptions{Image: packagedImage},
			},
			Services: &feastdevv1.FeatureStoreServices{},
		})
		if got != packagedImage {
			t.Fatalf("got %q, want %q", got, packagedImage)
		}
	})

	t.Run("falls back to RELATED_IMAGE_FEATURE_SERVER", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreSpec{
			Services: &feastdevv1.FeatureStoreServices{},
		})
		if got != envImage {
			t.Fatalf("got %q, want %q", got, envImage)
		}
	})

	t.Run("falls back to DefaultImage", func(t *testing.T) {
		_ = os.Unsetenv(feastServerImageVar)
		got := getInitContainerImage(nil)
		if got != DefaultImage {
			t.Fatalf("got %q, want %q", got, DefaultImage)
		}
	})

	t.Run("ignores empty initImage", func(t *testing.T) {
		t.Setenv(feastServerImageVar, envImage)
		got := getInitContainerImage(&feastdevv1.FeatureStoreSpec{
			FeastProjectDir: &feastdevv1.FeastProjectDir{
				Packaged: &feastdevv1.FeastPackagedOptions{Image: packagedImage},
			},
			Services: &feastdevv1.FeatureStoreServices{
				InitImage: ptr.To(""),
			},
		})
		if got != packagedImage {
			t.Fatalf("got %q, want %q", got, packagedImage)
		}
	})
}

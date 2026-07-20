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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
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
	// pod, letting a registry-only or offline-only FeatureStore skip it while
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

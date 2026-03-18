/*
Copyright 2026 Feast Community.

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
	"context"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var _ = Describe("ServiceMonitor", func() {
	var (
		featureStore       *feastdevv1.FeatureStore
		feast              *FeastServices
		typeNamespacedName types.NamespacedName
		ctx                context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{
			Name:      "sm-test-fs",
			Namespace: "default",
		}

		featureStore = &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "smtestproject",
				Services: &feastdevv1.FeatureStoreServices{
					OnlineStore: &feastdevv1.OnlineStore{
						Server: &feastdevv1.ServerConfigs{
							ContainerConfigs: feastdevv1.ContainerConfigs{
								DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
									Image: ptr.To("test-image"),
								},
							},
						},
					},
					Registry: &feastdevv1.Registry{
						Local: &feastdevv1.LocalRegistryConfig{
							Server: &feastdevv1.RegistryServerConfigs{
								ServerConfigs: feastdevv1.ServerConfigs{
									ContainerConfigs: feastdevv1.ContainerConfigs{
										DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
											Image: ptr.To("test-image"),
										},
									},
								},
								GRPC: ptr.To(true),
							},
						},
					},
				},
			},
		}

		Expect(k8sClient.Create(ctx, featureStore)).To(Succeed())

		feast = &FeastServices{
			Handler: handler.FeastHandler{
				Client:       k8sClient,
				Context:      ctx,
				Scheme:       k8sClient.Scheme(),
				FeatureStore: featureStore,
			},
		}

		Expect(feast.ApplyDefaults()).To(Succeed())
		applySpecToStatus(featureStore)
		feast.refreshFeatureStore(ctx, typeNamespacedName)
	})

	AfterEach(func() {
		testSetHasServiceMonitorCRD(false)
		Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
	})

	Describe("initServiceMonitor", func() {
		It("should create an unstructured ServiceMonitor with correct GVK and name", func() {
			sm := feast.initServiceMonitor()
			Expect(sm).NotTo(BeNil())
			Expect(sm.GetKind()).To(Equal("ServiceMonitor"))
			Expect(sm.GetAPIVersion()).To(Equal("monitoring.coreos.com/v1"))
			Expect(sm.GetName()).To(Equal(feast.GetFeastServiceName(OnlineFeastType)))
			Expect(sm.GetNamespace()).To(Equal(featureStore.Namespace))
		})
	})

	Describe("setServiceMonitor", func() {
		It("should populate the ServiceMonitor spec with correct selector and endpoint", func() {
			sm := feast.initServiceMonitor()
			Expect(feast.setServiceMonitor(sm)).To(Succeed())

			labels := sm.GetLabels()
			Expect(labels).To(HaveKeyWithValue(NameLabelKey, featureStore.Name))
			Expect(labels).To(HaveKeyWithValue(ServiceTypeLabelKey, string(OnlineFeastType)))

			spec, ok := sm.Object["spec"].(map[string]interface{})
			Expect(ok).To(BeTrue())

			endpoints, ok := spec["endpoints"].([]interface{})
			Expect(ok).To(BeTrue())
			Expect(endpoints).To(HaveLen(1))
			ep := endpoints[0].(map[string]interface{})
			Expect(ep["port"]).To(Equal("metrics"))
			Expect(ep["path"]).To(Equal("/metrics"))

			selector, ok := spec["selector"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			matchLabels := selector["matchLabels"].(map[string]interface{})
			Expect(matchLabels[NameLabelKey]).To(Equal(featureStore.Name))
			Expect(matchLabels[ServiceTypeLabelKey]).To(Equal(string(OnlineFeastType)))

			ownerRefs := sm.GetOwnerReferences()
			Expect(ownerRefs).To(HaveLen(1))
			Expect(ownerRefs[0].Name).To(Equal(featureStore.Name))
			Expect(*ownerRefs[0].Controller).To(BeTrue())
		})
	})

	Describe("createOrDeleteServiceMonitor", func() {
		It("should be a no-op when ServiceMonitor CRD is not available", func() {
			testSetHasServiceMonitorCRD(false)
			Expect(feast.createOrDeleteServiceMonitor()).To(Succeed())
		})

		It("should not error when metrics is not enabled and CRD is unavailable", func() {
			testSetHasServiceMonitorCRD(false)
			featureStore.Status.Applied.Services.OnlineStore.Server.Metrics = ptr.To(false)
			Expect(feast.createOrDeleteServiceMonitor()).To(Succeed())
		})
	})

	Describe("HasServiceMonitorCRD", func() {
		It("should return false by default", func() {
			testSetHasServiceMonitorCRD(false)
			Expect(HasServiceMonitorCRD()).To(BeFalse())
		})

		It("should return true when set", func() {
			testSetHasServiceMonitorCRD(true)
			Expect(HasServiceMonitorCRD()).To(BeTrue())
		})
	})
})

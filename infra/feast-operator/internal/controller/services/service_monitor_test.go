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

	Describe("buildServiceMonitorApplyConfig", func() {
		It("should build the correct SSA payload with labels, endpoints, selector, and owner reference", func() {
			sm := feast.buildServiceMonitorApplyConfig()

			Expect(*sm.APIVersion).To(Equal("monitoring.coreos.com/v1"))
			Expect(*sm.Kind).To(Equal("ServiceMonitor"))
			Expect(*sm.Name).To(Equal(feast.GetFeastServiceName(OnlineFeastType)))
			Expect(*sm.Namespace).To(Equal(featureStore.Namespace))

			Expect(sm.Labels).To(HaveKeyWithValue(NameLabelKey, featureStore.Name))
			Expect(sm.Labels).To(HaveKeyWithValue(ServiceTypeLabelKey, string(OnlineFeastType)))

			Expect(sm.OwnerReferences).To(HaveLen(1))
			ownerRef := sm.OwnerReferences[0]
			Expect(*ownerRef.APIVersion).To(Equal(feastdevv1.GroupVersion.String()))
			Expect(*ownerRef.Kind).To(Equal("FeatureStore"))
			Expect(*ownerRef.Name).To(Equal(featureStore.Name))
			Expect(*ownerRef.Controller).To(BeTrue())
			Expect(*ownerRef.BlockOwnerDeletion).To(BeTrue())

			Expect(sm.Spec).NotTo(BeNil())
			Expect(sm.Spec.Endpoints).To(HaveLen(1))
			Expect(*sm.Spec.Endpoints[0].Port).To(Equal("metrics"))
			Expect(*sm.Spec.Endpoints[0].Path).To(Equal("/metrics"))

			Expect(sm.Spec.Selector).NotTo(BeNil())
			Expect(sm.Spec.Selector.MatchLabels).To(HaveKeyWithValue(NameLabelKey, featureStore.Name))
			Expect(sm.Spec.Selector.MatchLabels).To(HaveKeyWithValue(ServiceTypeLabelKey, string(OnlineFeastType)))
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

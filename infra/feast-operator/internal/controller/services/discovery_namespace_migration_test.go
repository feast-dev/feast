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
	"encoding/json"
	"testing"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	migrationTestOperatorNamespace = "ml-platform"
	migrationTestStoreNamespace    = "team-a"
	migrationTestStoreName         = "fraud"
	migrationTestClientConfig      = "feast-fraud-client"
)

func newDiscoveryMigrationServices(t *testing.T, featureStore *feastdevv1.FeatureStore, objects ...client.Object) (*FeastServices, client.Client) {
	t.Helper()
	t.Setenv("FEAST_OPERATOR_NAMESPACE", migrationTestOperatorNamespace)
	t.Setenv("POD_NAMESPACE", "")

	previousIsOpenShift := isOpenShift
	isOpenShift = false
	t.Cleanup(func() {
		isOpenShift = previousIsOpenShift
	})

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	feast := &FeastServices{
		Handler: handler.FeastHandler{
			Client:       k8sClient,
			Context:      context.Background(),
			FeatureStore: featureStore,
		},
	}
	return feast, k8sClient
}

func namespaceRegistryConfigMap(t *testing.T, namespace string, data NamespaceRegistryData) *corev1.ConfigMap {
	t.Helper()
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal namespace registry data: %v", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NamespaceRegistryConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{NamespaceRegistryDataKey: string(encoded)},
	}
}

func openLineageConfigMap(t *testing.T, namespace string, data OpenLineageDiscoveryData) *corev1.ConfigMap {
	t.Helper()
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal OpenLineage discovery data: %v", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OpenLineageDiscoveryConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{OpenLineageDiscoveryEndpointsKey: string(encoded)},
	}
}

func readNamespaceRegistry(t *testing.T, k8sClient client.Client, namespace string) NamespaceRegistryData {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      NamespaceRegistryConfigMapName,
		Namespace: namespace,
	}, cm); err != nil {
		t.Fatalf("get namespace registry from %s: %v", namespace, err)
	}

	var data NamespaceRegistryData
	if err := json.Unmarshal([]byte(cm.Data[NamespaceRegistryDataKey]), &data); err != nil {
		t.Fatalf("unmarshal namespace registry from %s: %v", namespace, err)
	}
	return data
}

func readOpenLineageDiscovery(t *testing.T, k8sClient client.Client, namespace string) OpenLineageDiscoveryData {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{
		Name:      OpenLineageDiscoveryConfigMapName,
		Namespace: namespace,
	}, cm); err != nil {
		t.Fatalf("get OpenLineage discovery from %s: %v", namespace, err)
	}

	var data OpenLineageDiscoveryData
	if err := json.Unmarshal([]byte(cm.Data[OpenLineageDiscoveryEndpointsKey]), &data); err != nil {
		t.Fatalf("unmarshal OpenLineage discovery from %s: %v", namespace, err)
	}
	return data
}

func TestNamespaceRegistryKeepsLegacyConfigMapCurrent(t *testing.T) {
	featureStore := &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      migrationTestStoreName,
			Namespace: migrationTestStoreNamespace,
		},
		Status: feastdevv1.FeatureStoreStatus{
			ClientConfigMap: migrationTestClientConfig,
		},
	}
	currentConfigMap := namespaceRegistryConfigMap(t, migrationTestOperatorNamespace, NamespaceRegistryData{
		Namespaces: map[string][]string{"existing": {"current-client"}},
	})
	legacyConfigMap := namespaceRegistryConfigMap(t, DefaultKubernetesNamespace, NamespaceRegistryData{
		Namespaces: map[string][]string{
			"existing":                  {"legacy-client"},
			migrationTestStoreNamespace: {migrationTestClientConfig},
		},
	})
	feast, k8sClient := newDiscoveryMigrationServices(t, featureStore, currentConfigMap, legacyConfigMap)

	if err := feast.AddToNamespaceRegistry(); err != nil {
		t.Fatalf("add FeatureStore to namespace registry: %v", err)
	}
	for _, namespace := range []string{migrationTestOperatorNamespace, DefaultKubernetesNamespace} {
		data := readNamespaceRegistry(t, k8sClient, namespace)
		if got := data.Namespaces[migrationTestStoreNamespace]; len(got) != 1 || got[0] != migrationTestClientConfig {
			t.Errorf("registry in %s has %v, want the new client config", namespace, got)
		}
		if got := data.Namespaces["existing"]; len(got) != 1 {
			t.Errorf("registry in %s lost existing entries: %v", namespace, got)
		}
	}

	if err := feast.RemoveFromNamespaceRegistry(); err != nil {
		t.Fatalf("remove FeatureStore from namespace registry: %v", err)
	}
	for _, namespace := range []string{migrationTestOperatorNamespace, DefaultKubernetesNamespace} {
		data := readNamespaceRegistry(t, k8sClient, namespace)
		if _, found := data.Namespaces[migrationTestStoreNamespace]; found {
			t.Errorf("registry in %s still contains the deleted FeatureStore", namespace)
		}
		if got := data.Namespaces["existing"]; len(got) != 1 {
			t.Errorf("registry in %s lost unrelated entries: %v", namespace, got)
		}
	}
}

func TestOpenLineageDiscoveryKeepsLegacyConfigMapCurrent(t *testing.T) {
	featureStore := &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      migrationTestStoreName,
			Namespace: migrationTestStoreNamespace,
		},
		Status: feastdevv1.FeatureStoreStatus{
			Applied: feastdevv1.FeatureStoreSpec{
				OpenLineage: &feastdevv1.OpenLineageConfig{
					Consumer: &feastdevv1.OpenLineageConsumerConfig{
						Enabled:       true,
						LineageServer: &feastdevv1.LineageServerConfig{},
					},
				},
			},
		},
	}
	otherKey := "team-b/other"
	otherConsumer := OpenLineageEndpointInfo{
		URL:      "http://other.svc:6580",
		Endpoint: "api/v1/lineage",
	}
	storeKey := migrationTestStoreNamespace + "/" + migrationTestStoreName
	initialData := OpenLineageDiscoveryData{
		Consumers: map[string]OpenLineageEndpointInfo{
			otherKey: otherConsumer,
			storeKey: {
				URL:      "http://old-fraud.svc:6580",
				Endpoint: "api/v1/lineage",
			},
		},
	}
	currentConfigMap := openLineageConfigMap(t, migrationTestOperatorNamespace, initialData)
	legacyConfigMap := openLineageConfigMap(t, DefaultKubernetesNamespace, initialData)
	feast, k8sClient := newDiscoveryMigrationServices(t, featureStore, currentConfigMap, legacyConfigMap)

	if err := feast.AddToOpenLineageDiscovery(); err != nil {
		t.Fatalf("add FeatureStore to OpenLineage discovery: %v", err)
	}
	for _, namespace := range []string{migrationTestOperatorNamespace, DefaultKubernetesNamespace} {
		data := readOpenLineageDiscovery(t, k8sClient, namespace)
		if got := data.Consumers[storeKey]; got.URL == "" || got.Endpoint != "api/v1/lineage" {
			t.Errorf("OpenLineage discovery in %s has no updated entry for %s: %+v", namespace, storeKey, got)
		}
		if got := data.Consumers[otherKey]; got != otherConsumer {
			t.Errorf("OpenLineage discovery in %s changed unrelated consumer: %+v", namespace, got)
		}
	}

	if err := feast.RemoveFromOpenLineageDiscovery(); err != nil {
		t.Fatalf("remove FeatureStore from OpenLineage discovery: %v", err)
	}
	for _, namespace := range []string{migrationTestOperatorNamespace, DefaultKubernetesNamespace} {
		data := readOpenLineageDiscovery(t, k8sClient, namespace)
		if _, found := data.Consumers[storeKey]; found {
			t.Errorf("OpenLineage discovery in %s still contains the deleted FeatureStore", namespace)
		}
		if got := data.Consumers[otherKey]; got != otherConsumer {
			t.Errorf("OpenLineage discovery in %s lost unrelated consumers: %+v", namespace, got)
		}
	}
}

func TestDiscoveryConfigMapNamespacesSkipsAbsentLegacyMap(t *testing.T) {
	feast, _ := newDiscoveryMigrationServices(t, &feastdevv1.FeatureStore{})
	namespaces, err := feast.discoveryConfigMapNamespaces(migrationTestOperatorNamespace, NamespaceRegistryConfigMapName)
	if err != nil {
		t.Fatalf("get discovery namespaces: %v", err)
	}
	if len(namespaces) != 1 || namespaces[0] != migrationTestOperatorNamespace {
		t.Fatalf("got namespaces %v, want only %q", namespaces, migrationTestOperatorNamespace)
	}
}

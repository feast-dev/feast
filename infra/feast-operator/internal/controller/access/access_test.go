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

package access

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	return s
}

func newNamespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func getNamespace(t *testing.T, c client.Client, name string) *corev1.Namespace {
	t.Helper()
	ns := &corev1.Namespace{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, ns); err != nil {
		t.Fatalf("Failed to get namespace %s: %v", name, err)
	}
	return ns
}

func TestEnsureNamespaceLabel_AddsLabel(t *testing.T) {
	ns := newNamespace("test-ns", nil)
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := EnsureNamespaceLabel(context.Background(), c, "test-ns"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := getNamespace(t, c, "test-ns")
	if updated.Labels[FeastNamespaceLabelKey] != FeastNamespaceLabelValue {
		t.Fatalf("expected label %s=%s, got %v", FeastNamespaceLabelKey, FeastNamespaceLabelValue, updated.Labels)
	}
}

func TestEnsureNamespaceLabel_AlreadyLabeled(t *testing.T) {
	ns := newNamespace("test-ns", map[string]string{FeastNamespaceLabelKey: FeastNamespaceLabelValue})
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := EnsureNamespaceLabel(context.Background(), c, "test-ns"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := getNamespace(t, c, "test-ns")
	if updated.Labels[FeastNamespaceLabelKey] != FeastNamespaceLabelValue {
		t.Fatalf("label should still be present")
	}
}

func TestEnsureNamespaceLabel_NamespaceNotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()

	if err := EnsureNamespaceLabel(context.Background(), c, "missing-ns"); err != nil {
		t.Fatalf("expected nil error for missing namespace, got: %v", err)
	}
}

func TestEnsureNamespaceLabel_PreservesExistingLabels(t *testing.T) {
	ns := newNamespace("test-ns", map[string]string{"existing": "label"})
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := EnsureNamespaceLabel(context.Background(), c, "test-ns"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := getNamespace(t, c, "test-ns")
	if updated.Labels["existing"] != "label" {
		t.Fatal("existing label was removed")
	}
	if updated.Labels[FeastNamespaceLabelKey] != FeastNamespaceLabelValue {
		t.Fatal("feast label was not added")
	}
}

func TestRemoveNamespaceLabelIfLast_RemovesWhenZero(t *testing.T) {
	ns := newNamespace("test-ns", map[string]string{FeastNamespaceLabelKey: FeastNamespaceLabelValue})
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := RemoveNamespaceLabelIfLast(context.Background(), c, "test-ns", 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := getNamespace(t, c, "test-ns")
	if _, ok := updated.Labels[FeastNamespaceLabelKey]; ok {
		t.Fatal("label should have been removed")
	}
}

func TestRemoveNamespaceLabelIfLast_KeepsWhenOthersExist(t *testing.T) {
	ns := newNamespace("test-ns", map[string]string{FeastNamespaceLabelKey: FeastNamespaceLabelValue})
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := RemoveNamespaceLabelIfLast(context.Background(), c, "test-ns", 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := getNamespace(t, c, "test-ns")
	if updated.Labels[FeastNamespaceLabelKey] != FeastNamespaceLabelValue {
		t.Fatal("label should not have been removed when other FeatureStores exist")
	}
}

func TestRemoveNamespaceLabelIfLast_NoLabelNoOp(t *testing.T) {
	ns := newNamespace("test-ns", nil)
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(ns).Build()

	if err := RemoveNamespaceLabelIfLast(context.Background(), c, "test-ns", 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRemoveNamespaceLabelIfLast_NamespaceNotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()

	if err := RemoveNamespaceLabelIfLast(context.Background(), c, "missing-ns", 0); err != nil {
		t.Fatalf("expected nil error for missing namespace, got: %v", err)
	}
}

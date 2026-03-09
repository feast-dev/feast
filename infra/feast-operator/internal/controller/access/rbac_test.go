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
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/registry"
)

func newRBACScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = rbacv1.AddToScheme(s)
	return s
}

func TestBuildSubjects_GroupBasedPolicy(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).Build()
	policies := []registry.PermissionPolicy{
		{Groups: []string{"admins", "data-team"}},
	}
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 2 {
		t.Fatalf("expected 2 subjects, got %d", len(subjects))
	}
	assertSubject(t, subjects[0], "Group", "admins")
	assertSubject(t, subjects[1], "Group", "data-team")
}

func TestBuildSubjects_NamespaceBasedPolicy(t *testing.T) {
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb1", Namespace: "ds-project"},
		Subjects: []rbacv1.Subject{
			{Kind: "User", Name: "alice", APIGroup: rbacv1.GroupName},
			{Kind: "Group", Name: "team-x", APIGroup: rbacv1.GroupName},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).WithObjects(rb).Build()
	policies := []registry.PermissionPolicy{
		{Namespaces: []string{"ds-project"}},
	}
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 2 {
		t.Fatalf("expected 2 subjects, got %d", len(subjects))
	}
}

func TestBuildSubjects_CombinedPolicy(t *testing.T) {
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb1", Namespace: "ns1"},
		Subjects: []rbacv1.Subject{
			{Kind: "User", Name: "bob", APIGroup: rbacv1.GroupName},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).WithObjects(rb).Build()
	policies := []registry.PermissionPolicy{
		{Groups: []string{"engineers"}, Namespaces: []string{"ns1"}},
	}
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 2 {
		t.Fatalf("expected 2 subjects (1 group + 1 user), got %d", len(subjects))
	}
}

func TestBuildSubjects_DeduplicatesGroups(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).Build()
	policies := []registry.PermissionPolicy{
		{Groups: []string{"admins"}},
		{Groups: []string{"admins", "other"}},
	}
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 2 {
		t.Fatalf("expected 2 unique subjects, got %d: %v", len(subjects), subjects)
	}
}

func TestBuildSubjects_DeduplicatesNamespaceSubjects(t *testing.T) {
	rb1 := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb1", Namespace: "ns1"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: "alice", APIGroup: rbacv1.GroupName}},
	}
	rb2 := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb2", Namespace: "ns1"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: "alice", APIGroup: rbacv1.GroupName}},
	}
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).WithObjects(rb1, rb2).Build()
	policies := []registry.PermissionPolicy{
		{Namespaces: []string{"ns1"}},
	}
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 1 {
		t.Fatalf("expected 1 deduplicated subject, got %d", len(subjects))
	}
}

func TestBuildSubjects_EmptyPolicies(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).Build()
	subjects := buildSubjects(context.Background(), c, nil)
	if len(subjects) != 0 {
		t.Fatalf("expected 0 subjects for nil policies, got %d", len(subjects))
	}
}

func TestBuildSubjects_NonexistentNamespace(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).Build()
	policies := []registry.PermissionPolicy{
		{Namespaces: []string{"nonexistent"}},
	}
	// Should not error, just return no subjects from that namespace
	subjects := buildSubjects(context.Background(), c, policies)
	if len(subjects) != 0 {
		t.Fatalf("expected 0 subjects for nonexistent namespace, got %d", len(subjects))
	}
}

func TestReconcileAutoAccessRBAC_CreatesAllResources(t *testing.T) {
	scheme := newRBACScheme()
	ns := newNamespace("feast-ns", nil)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

	policies := []registry.PermissionPolicy{
		{Groups: []string{"data-scientists"}},
	}
	err := ReconcileAutoAccessRBAC(context.Background(), c, scheme, nil, "feast-ns", "my-feast", "feast-client-config", policies)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify ClusterRole
	cr := &rbacv1.ClusterRole{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: FeastDiscoverClusterRoleName}, cr); err != nil {
		t.Fatalf("Failed to get ClusterRole: %v", err)
	}
	if len(cr.Rules) != 1 || cr.Rules[0].Resources[0] != "namespaces" {
		t.Fatalf("unexpected ClusterRole rules: %v", cr.Rules)
	}

	// Verify ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "feast-feast-ns-my-feast-discover"}, crb); err != nil {
		t.Fatalf("Failed to get ClusterRoleBinding: %v", err)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != "data-scientists" {
		t.Fatalf("unexpected ClusterRoleBinding subjects: %v", crb.Subjects)
	}

	// Verify Role
	role := &rbacv1.Role{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "feast-ns", Name: "feast-my-feast-viewer"}, role); err != nil {
		t.Fatalf("Failed to get Role: %v", err)
	}
	if len(role.Rules) != 1 || role.Rules[0].ResourceNames[0] != "feast-client-config" {
		t.Fatalf("unexpected Role rules: %v", role.Rules)
	}

	// Verify RoleBinding
	rb := &rbacv1.RoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "feast-ns", Name: "feast-my-feast-viewer"}, rb); err != nil {
		t.Fatalf("Failed to get RoleBinding: %v", err)
	}
	if len(rb.Subjects) != 1 || rb.Subjects[0].Name != "data-scientists" {
		t.Fatalf("unexpected RoleBinding subjects: %v", rb.Subjects)
	}
}

func TestReconcileAutoAccessRBAC_NoSubjects(t *testing.T) {
	scheme := newRBACScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Empty policies -> no subjects -> should be a no-op
	err := ReconcileAutoAccessRBAC(context.Background(), c, scheme, nil, "ns", "name", "cm", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	crb := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "feast-ns-name-discover"}, crb); err == nil {
		t.Fatal("ClusterRoleBinding should not exist when there are no subjects")
	}
}

func TestReconcileAutoAccessRBAC_UpdatesExistingResources(t *testing.T) {
	scheme := newRBACScheme()
	ns := newNamespace("feast-ns", nil)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

	// First reconcile with one group
	policies := []registry.PermissionPolicy{{Groups: []string{"group-a"}}}
	if err := ReconcileAutoAccessRBAC(context.Background(), c, scheme, nil, "feast-ns", "fs", "cm", policies); err != nil {
		t.Fatalf("unexpected error on first reconcile: %v", err)
	}

	// Second reconcile with different group
	policies = []registry.PermissionPolicy{{Groups: []string{"group-b"}}}
	if err := ReconcileAutoAccessRBAC(context.Background(), c, scheme, nil, "feast-ns", "fs", "cm", policies); err != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err)
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "feast-feast-ns-fs-discover"}, crb); err != nil {
		t.Fatalf("Failed to get ClusterRoleBinding: %v", err)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != "group-b" {
		t.Fatalf("expected updated subject group-b, got: %v", crb.Subjects)
	}
}

func TestCleanupAutoAccessRBAC(t *testing.T) {
	scheme := newRBACScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "feast-ns-my-feast-discover"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "feast-my-feast-viewer", Namespace: "ns"},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "feast-my-feast-viewer", Namespace: "ns"},
		},
	).Build()

	if err := CleanupAutoAccessRBAC(context.Background(), c, "ns", "my-feast"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "feast-ns-my-feast-discover"}, crb); err == nil {
		t.Fatal("ClusterRoleBinding should have been deleted")
	}
	role := &rbacv1.Role{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "feast-my-feast-viewer"}, role); err == nil {
		t.Fatal("Role should have been deleted")
	}
	rb := &rbacv1.RoleBinding{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "feast-my-feast-viewer"}, rb); err == nil {
		t.Fatal("RoleBinding should have been deleted")
	}
}

func TestCleanupAutoAccessRBAC_AlreadyGone(t *testing.T) {
	scheme := newRBACScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Should not error when resources don't exist
	if err := CleanupAutoAccessRBAC(context.Background(), c, "ns", "name"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureFeastDiscoverClusterRole_Idempotent(t *testing.T) {
	scheme := newRBACScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	if err := ensureFeastDiscoverClusterRole(context.Background(), c); err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if err := ensureFeastDiscoverClusterRole(context.Background(), c); err != nil {
		t.Fatalf("second call (idempotent) failed: %v", err)
	}
	cr := &rbacv1.ClusterRole{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: FeastDiscoverClusterRoleName}, cr); err != nil {
		t.Fatalf("Failed to get ClusterRole: %v", err)
	}
}

func TestListSubjectsInNamespace(t *testing.T) {
	rb1 := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb1", Namespace: "ns1"},
		Subjects: []rbacv1.Subject{
			{Kind: "User", Name: "alice", APIGroup: rbacv1.GroupName},
			{Kind: "Group", Name: "team-a", APIGroup: rbacv1.GroupName},
		},
	}
	rb2 := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb2", Namespace: "ns1"},
		Subjects: []rbacv1.Subject{
			{Kind: "User", Name: "alice", APIGroup: rbacv1.GroupName},
			{Kind: "User", Name: "bob", APIGroup: rbacv1.GroupName},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newRBACScheme()).WithObjects(rb1, rb2).Build()

	subjects, err := listSubjectsInNamespace(context.Background(), c, "ns1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// alice (deduplicated), team-a, bob = 3 unique subjects
	if len(subjects) != 3 {
		t.Fatalf("expected 3 unique subjects, got %d: %v", len(subjects), subjects)
	}
}

func assertSubject(t *testing.T, s rbacv1.Subject, expectedKind, expectedName string) {
	t.Helper()
	if s.Kind != expectedKind || s.Name != expectedName {
		t.Fatalf("expected subject %s/%s, got %s/%s", expectedKind, expectedName, s.Kind, s.Name)
	}
}

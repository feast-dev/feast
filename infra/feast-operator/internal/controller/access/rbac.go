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
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/registry"
)

const (
	FeastDiscoverClusterRoleName = "feast-discover-namespaces"
)

// ReconcileAutoAccessRBAC creates or updates RBAC so subjects from registry permissions
// get view access to the Feature Store namespace and client ConfigMap.
func ReconcileAutoAccessRBAC(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, name, clientConfigMapName string, policies []registry.PermissionPolicy) error {
	subjects := buildSubjects(ctx, c, policies)
	if len(subjects) == 0 {
		return nil
	}
	if err := ensureFeastDiscoverClusterRole(ctx, c); err != nil {
		return err
	}
	clusterBindingName := "feast-" + namespace + "-" + name + "-discover"
	if err := reconcileClusterRoleBinding(ctx, c, scheme, owner, clusterBindingName, subjects); err != nil {
		return err
	}
	roleName := "feast-" + name + "-viewer"
	if err := reconcileViewerRole(ctx, c, scheme, owner, namespace, roleName, clientConfigMapName); err != nil {
		return err
	}
	if err := reconcileViewerRoleBinding(ctx, c, scheme, owner, namespace, roleName, subjects); err != nil {
		return err
	}
	return nil
}

func buildSubjects(ctx context.Context, c client.Client, policies []registry.PermissionPolicy) []rbacv1.Subject {
	seen := make(map[string]struct{})
	var subjects []rbacv1.Subject
	for _, p := range policies {
		for _, g := range p.Groups {
			key := "Group:" + g
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			subjects = append(subjects, rbacv1.Subject{
				APIGroup: rbacv1.GroupName,
				Kind:     "Group",
				Name:     g,
			})
		}
		for _, ns := range p.Namespaces {
			subs, err := listSubjectsInNamespace(ctx, c, ns)
			if err != nil {
				log.FromContext(ctx).V(1).Info("Failed to list RoleBindings in namespace for NamespaceBasedPolicy", "namespace", ns, "error", err)
				continue
			}
			for _, s := range subs {
				key := subjectKey(s)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				subjects = append(subjects, s)
			}
		}
	}
	return subjects
}

func subjectKey(s rbacv1.Subject) string {
	apiGroup := s.APIGroup
	if apiGroup == "" {
		apiGroup = "rbac.authorization.k8s.io"
	}
	return s.Kind + ":" + apiGroup + ":" + s.Name + ":" + s.Namespace
}

func listSubjectsInNamespace(ctx context.Context, c client.Client, namespace string) ([]rbacv1.Subject, error) {
	var list rbacv1.RoleBindingList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var out []rbacv1.Subject
	seen := make(map[string]struct{})
	for i := range list.Items {
		for _, s := range list.Items[i].Subjects {
			key := subjectKey(s)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, s)
		}
	}
	return out, nil
}

func ensureFeastDiscoverClusterRole(ctx context.Context, c client.Client) error {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: FeastDiscoverClusterRoleName},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, cr, func() error {
		cr.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"namespaces"},
				Verbs:     []string{"get", "list", "watch"},
			},
		}
		return nil
	})
	return err
}

func reconcileClusterRoleBinding(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, name string, subjects []rbacv1.Subject) error {
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     FeastDiscoverClusterRoleName,
		}
		crb.Subjects = subjects
		if owner != nil && scheme != nil {
			return controllerutil.SetControllerReference(owner, crb, scheme)
		}
		return nil
	})
	return err
}

func reconcileViewerRole(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, roleName, configMapName string) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{configMapName},
				Verbs:         []string{"get", "list", "watch"},
			},
		}
		if owner != nil && scheme != nil {
			return controllerutil.SetControllerReference(owner, role, scheme)
		}
		return nil
	})
	return err
}

func reconcileViewerRoleBinding(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace, roleName string, subjects []rbacv1.Subject) error {
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, rb, func() error {
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		}
		rb.Subjects = subjects
		if owner != nil && scheme != nil {
			return controllerutil.SetControllerReference(owner, rb, scheme)
		}
		return nil
	})
	return err
}

// CleanupAutoAccessRBAC removes the auto-access ClusterRoleBinding, Role, and RoleBinding.
func CleanupAutoAccessRBAC(ctx context.Context, c client.Client, namespace, name string) error {
	clusterBindingName := "feast-" + namespace + "-" + name + "-discover"
	crb := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(ctx, client.ObjectKey{Name: clusterBindingName}, crb); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else if err := c.Delete(ctx, crb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", clusterBindingName, err)
	}
	roleName := "feast-" + name + "-viewer"
	role := &rbacv1.Role{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: roleName}, role); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else if err := c.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Role %s: %w", roleName, err)
	}
	rb := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: roleName}, rb); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else if err := c.Delete(ctx, rb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete RoleBinding %s: %w", roleName, err)
	}
	return nil
}

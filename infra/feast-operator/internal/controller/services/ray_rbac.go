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
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// rayEngineType is the Feast Python SDK value for batch_engine.type when the
// Ray compute engine is selected (see RayComputeEngineConfig.type).
const rayEngineType = "ray.engine"

// kubeRayRBACSuffix is appended to the FeatureStore name to form the namespaced
// Role and RoleBinding that grant the Feast service account access to KubeRay
// resources.
const kubeRayRBACSuffix = "-kuberay"

// usesKubeRay reports whether the FeatureStore's batch engine is configured to
// connect to a KubeRay cluster. It reads the user-supplied batch engine
// ConfigMap and returns true only when the resolved config has both
// type == "ray.engine" and use_kuberay == true.
func (feast *FeastServices) usesKubeRay() (bool, error) {
	spec := feast.Handler.FeatureStore.Status.Applied
	if spec.BatchEngine == nil || spec.BatchEngine.ConfigMapRef == nil {
		return false, nil
	}
	cfg, err := feast.extractConfigFromConfigMap(spec.BatchEngine.ConfigMapRef.Name, spec.BatchEngine.ConfigMapKey)
	if err != nil {
		return false, err
	}
	if engineType, _ := cfg["type"].(string); engineType != rayEngineType {
		return false, nil
	}
	useKubeRay, _ := cfg["use_kuberay"].(bool)
	return useKubeRay, nil
}

// applyOrDeleteKubeRayRBAC creates the KubeRay Role and RoleBinding when the
// batch engine is configured for KubeRay, and deletes them otherwise. The
// resources are owner-referenced to the FeatureStore so they are garbage
// collected with the CR.
func (feast *FeastServices) applyOrDeleteKubeRayRBAC() error {
	enabled, err := feast.usesKubeRay()
	if err != nil {
		return err
	}
	if !enabled {
		if err := feast.Handler.DeleteOwnedFeastObj(feast.initKubeRayRoleBinding()); err != nil {
			return err
		}
		return feast.Handler.DeleteOwnedFeastObj(feast.initKubeRayRole())
	}
	if err := feast.createKubeRayRole(); err != nil {
		return err
	}
	return feast.createKubeRayRoleBinding()
}

func (feast *FeastServices) createKubeRayRole() error {
	logger := log.FromContext(feast.Handler.Context)
	role := feast.initKubeRayRole()
	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, role, func() error {
		return feast.setKubeRayRole(role)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createKubeRayRoleBinding() error {
	logger := log.FromContext(feast.Handler.Context)
	binding := feast.initKubeRayRoleBinding()
	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, binding, func() error {
		return feast.setKubeRayRoleBinding(binding)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "RoleBinding", binding.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) initKubeRayRole() *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.getKubeRayRBACName(),
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	return role
}

func (feast *FeastServices) setKubeRayRole(role *rbacv1.Role) error {
	role.Labels = feast.getKubeRayLabels()
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups: []string{"ray.io"},
			Resources: []string{"rayclusters"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"secrets"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "delete"},
		},
	}
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, role, feast.Handler.Scheme)
}

func (feast *FeastServices) initKubeRayRoleBinding() *rbacv1.RoleBinding {
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.getKubeRayRBACName(),
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	binding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	return binding
}

func (feast *FeastServices) setKubeRayRoleBinding(binding *rbacv1.RoleBinding) error {
	binding.Labels = feast.getKubeRayLabels()
	binding.Subjects = []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      GetFeastName(feast.Handler.FeatureStore),
		Namespace: feast.Handler.FeatureStore.Namespace,
	}}
	binding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     feast.getKubeRayRBACName(),
	}
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, binding, feast.Handler.Scheme)
}

func (feast *FeastServices) getKubeRayRBACName() string {
	return GetFeastName(feast.Handler.FeatureStore) + kubeRayRBACSuffix
}

func (feast *FeastServices) getKubeRayLabels() map[string]string {
	return map[string]string{
		NameLabelKey:      feast.Handler.FeatureStore.Name,
		ManagedByLabelKey: ManagedByLabelValue,
	}
}

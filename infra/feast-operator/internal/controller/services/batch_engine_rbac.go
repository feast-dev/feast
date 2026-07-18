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
	"embed"
	"fmt"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

const (
	BatchEngineFeastType FeastServiceType = "batch-engine"
	BatchDriverFeastType FeastServiceType = "batch-driver"
)

//go:embed rbac_templates/*.yaml
var batchEngineRBACTemplates embed.FS

// BatchEngineRBACTemplate declares RBAC requirements for a batch compute engine.
type BatchEngineRBACTemplate struct {
	EngineType string          `json:"engine_type" yaml:"engine_type"`
	Server     *RBACRoleSpec   `json:"server,omitempty" yaml:"server,omitempty"`
	Driver     *DriverRBACSpec `json:"driver,omitempty" yaml:"driver,omitempty"`
}

// RBACRoleSpec defines policy rules for a Role.
type RBACRoleSpec struct {
	Rules []rbacv1.PolicyRule `json:"rules" yaml:"rules"`
}

// DriverRBACSpec defines policy rules and optional SA creation for a driver Role.
type DriverRBACSpec struct {
	CreateServiceAccount bool                `json:"create_service_account" yaml:"create_service_account"`
	Rules                []rbacv1.PolicyRule `json:"rules" yaml:"rules"`
}

func loadBatchEngineTemplate(engineType string) (*BatchEngineRBACTemplate, error) {
	data, err := batchEngineRBACTemplates.ReadFile(
		"rbac_templates/" + engineType + ".yaml",
	)
	if err != nil {
		return nil, nil
	}
	var tmpl BatchEngineRBACTemplate
	if err := yaml.Unmarshal(data, &tmpl); err != nil {
		return nil, fmt.Errorf("failed to parse RBAC template for engine %q: %w", engineType, err)
	}
	return &tmpl, nil
}

func (feast *FeastServices) reconcileBatchEngineRBAC() error {
	config, ok := feast.getBatchEngineConfig()
	if !ok {
		return feast.deleteBatchEngineRBAC()
	}

	engineType, _ := config["type"].(string)
	if engineType == "" {
		return feast.deleteBatchEngineRBAC()
	}

	tmpl, err := loadBatchEngineTemplate(engineType)
	if err != nil {
		return err
	}
	if tmpl == nil {
		return feast.deleteBatchEngineRBAC()
	}

	if tmpl.Server != nil {
		if err := feast.ensureBatchEngineRole(BatchEngineFeastType, tmpl.Server.Rules); err != nil {
			return err
		}
		if err := feast.ensureBatchEngineRoleBinding(BatchEngineFeastType, feast.initFeastSA().Name); err != nil {
			return err
		}
	}

	if tmpl.Driver != nil {
		driverSAName := resolveBatchDriverSAName(feast.Handler.FeatureStore, config)
		if tmpl.Driver.CreateServiceAccount {
			if err := feast.ensureBatchDriverServiceAccount(driverSAName); err != nil {
				return err
			}
		}
		if err := feast.ensureBatchEngineRole(BatchDriverFeastType, tmpl.Driver.Rules); err != nil {
			return err
		}
		if err := feast.ensureBatchEngineRoleBinding(BatchDriverFeastType, driverSAName); err != nil {
			return err
		}
	}

	return nil
}

// getBatchEngineConfig returns the parsed batch-engine ConfigMap data.
// ok=false means no batch engine is configured or the ConfigMap is unreadable.
func (feast *FeastServices) getBatchEngineConfig() (map[string]interface{}, bool) {
	appliedSpec := feast.Handler.FeatureStore.Status.Applied
	if appliedSpec.BatchEngine == nil || appliedSpec.BatchEngine.ConfigMapRef == nil {
		return nil, false
	}

	configMapKey := appliedSpec.BatchEngine.ConfigMapKey
	if configMapKey == "" {
		configMapKey = "config"
	}

	cm, err := feast.getConfigMap(appliedSpec.BatchEngine.ConfigMapRef.Name)
	if err != nil {
		return nil, false
	}

	data, found := cm.Data[configMapKey]
	if !found {
		return nil, false
	}

	var config map[string]interface{}
	if err := yaml.Unmarshal([]byte(data), &config); err != nil {
		return nil, false
	}
	return config, true
}

// resolveBatchDriverSAName returns the ServiceAccount name for the Spark driver.
// If batch engine config sets a non-empty service_account, that value wins.
// Otherwise defaults to feast-<FeatureStoreName>-batch-driver (same name used for RBAC).
func resolveBatchDriverSAName(featureStore *feastdevv1.FeatureStore, config map[string]interface{}) string {
	if sa, ok := config["service_account"].(string); ok && sa != "" {
		return sa
	}
	return GetFeastServiceName(featureStore, BatchDriverFeastType)
}

func (feast *FeastServices) ensureBatchEngineRole(feastType FeastServiceType, rules []rbacv1.PolicyRule) error {
	logger := log.FromContext(feast.Handler.Context)
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.GetFeastServiceName(feastType),
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))

	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, role, func() error {
		role.Labels = feast.getFeastTypeLabels(feastType)
		role.Rules = rules
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, role, feast.Handler.Scheme)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) ensureBatchEngineRoleBinding(feastType FeastServiceType, saName string) error {
	logger := log.FromContext(feast.Handler.Context)
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.GetFeastServiceName(feastType),
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))

	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, roleBinding, func() error {
		roleBinding.Labels = feast.getFeastTypeLabels(feastType)
		roleBinding.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      saName,
			Namespace: feast.Handler.FeatureStore.Namespace,
		}}
		roleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     feast.GetFeastServiceName(feastType),
		}
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, roleBinding, feast.Handler.Scheme)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "RoleBinding", roleBinding.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) ensureBatchDriverServiceAccount(saName string) error {
	logger := log.FromContext(feast.Handler.Context)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	sa.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))

	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, sa, func() error {
		if sa.Labels == nil {
			sa.Labels = map[string]string{}
		}
		for k, v := range feast.getFeastTypeLabels(BatchDriverFeastType) {
			sa.Labels[k] = v
		}
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, sa, feast.Handler.Scheme)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ServiceAccount", sa.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) deleteBatchEngineRBAC() error {
	serverRoleName := feast.GetFeastServiceName(BatchEngineFeastType)
	driverRoleName := feast.GetFeastServiceName(BatchDriverFeastType)
	ns := feast.Handler.FeatureStore.Namespace

	serverRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: serverRoleName, Namespace: ns},
	}
	serverRoleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	if err := feast.Handler.DeleteOwnedFeastObj(serverRoleBinding); err != nil {
		return err
	}

	serverRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: serverRoleName, Namespace: ns},
	}
	serverRole.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	if err := feast.Handler.DeleteOwnedFeastObj(serverRole); err != nil {
		return err
	}

	driverRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: driverRoleName, Namespace: ns},
	}
	driverRoleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	if err := feast.Handler.DeleteOwnedFeastObj(driverRoleBinding); err != nil {
		return err
	}

	driverRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: driverRoleName, Namespace: ns},
	}
	driverRole.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	if err := feast.Handler.DeleteOwnedFeastObj(driverRole); err != nil {
		return err
	}

	driverSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: driverRoleName, Namespace: ns},
	}
	driverSA.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	return feast.Handler.DeleteOwnedFeastObj(driverSA)
}

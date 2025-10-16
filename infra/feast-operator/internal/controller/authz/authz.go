package authz

import (
	"context"
	"slices"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	rbacv1 "k8s.io/api/rbac/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Deploy the feast authorization
func (authz *FeastAuthorization) Deploy() error {
	if authz.isKubernetesAuth() {
		return authz.deployKubernetesAuth()
	}

	authz.removeOrphanedRoles()
	_ = authz.Handler.DeleteOwnedFeastObj(authz.initFeastRole())
	_ = authz.Handler.DeleteOwnedFeastObj(authz.initFeastRoleBinding())
	apimeta.RemoveStatusCondition(&authz.Handler.FeatureStore.Status.Conditions, feastKubernetesAuthConditions[metav1.ConditionTrue].Type)
	return nil
}

func (authz *FeastAuthorization) isKubernetesAuth() bool {
	authzConfig := authz.Handler.FeatureStore.Status.Applied.AuthzConfig
	return authzConfig != nil && authzConfig.KubernetesAuthz != nil
}

func (authz *FeastAuthorization) deployKubernetesAuth() error {
	if authz.isKubernetesAuth() {
		authz.removeOrphanedRoles()

		// Create namespace-scoped RBAC resources
		if err := authz.createFeastRole(); err != nil {
			return authz.setFeastKubernetesAuthCondition(err)
		}
		if err := authz.createFeastRoleBinding(); err != nil {
			return authz.setFeastKubernetesAuthCondition(err)
		}

		// Create cluster-scoped RBAC resources (separate from namespace resources)
		if err := authz.createFeastClusterRole(); err != nil {
			return authz.setFeastKubernetesAuthCondition(err)
		}
		if err := authz.createFeastClusterRoleBinding(); err != nil {
			return authz.setFeastKubernetesAuthCondition(err)
		}

		// Create custom auth roles
		for _, roleName := range authz.Handler.FeatureStore.Status.Applied.AuthzConfig.KubernetesAuthz.Roles {
			if err := authz.createAuthRole(roleName); err != nil {
				return authz.setFeastKubernetesAuthCondition(err)
			}
		}
	}
	return authz.setFeastKubernetesAuthCondition(nil)
}

func (authz *FeastAuthorization) removeOrphanedRoles() {
	roleList := &rbacv1.RoleList{}
	err := authz.Handler.Client.List(context.TODO(), roleList, &client.ListOptions{
		Namespace:     authz.Handler.FeatureStore.Namespace,
		LabelSelector: labels.SelectorFromSet(authz.getLabels()),
	})
	if err != nil {
		return
	}

	desiredRoles := []string{}
	if authz.isKubernetesAuth() {
		desiredRoles = authz.Handler.FeatureStore.Status.Applied.AuthzConfig.KubernetesAuthz.Roles
	}
	for _, role := range roleList.Items {
		roleName := role.Name
		if roleName != authz.getFeastRoleName() && !slices.Contains(desiredRoles, roleName) {
			_ = authz.Handler.DeleteOwnedFeastObj(authz.initAuthRole(roleName))
		}
	}
}

func (authz *FeastAuthorization) createFeastRole() error {
	logger := log.FromContext(authz.Handler.Context)
	role := authz.initFeastRole()
	if op, err := controllerutil.CreateOrUpdate(authz.Handler.Context, authz.Handler.Client, role, controllerutil.MutateFn(func() error {
		return authz.setFeastRole(role)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}

	return nil
}

func (authz *FeastAuthorization) createFeastClusterRole() error {
	logger := log.FromContext(authz.Handler.Context)
	clusterRole := authz.initFeastClusterRole()
	if op, err := controllerutil.CreateOrUpdate(authz.Handler.Context, authz.Handler.Client, clusterRole, controllerutil.MutateFn(func() error {
		return authz.setFeastClusterRole(clusterRole)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ClusterRole", clusterRole.Name, "operation", op)
	}

	return nil
}

func (authz *FeastAuthorization) initFeastClusterRole() *rbacv1.ClusterRole {
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: authz.getFeastClusterRoleName()},
	}
	clusterRole.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRole"))
	return clusterRole
}

func (authz *FeastAuthorization) setFeastClusterRole(clusterRole *rbacv1.ClusterRole) error {
	clusterRole.Labels = authz.getLabels()
	clusterRole.Rules = []rbacv1.PolicyRule{
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"rolebindings"},
			Verbs:     []string{"list"},
		},
		{
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"tokenreviews"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"subjectaccessreviews"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"namespaces"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"clusterroles"},
			Verbs:     []string{"get", "list"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"clusterrolebindings"},
			Verbs:     []string{"get", "list"},
		},
	}
	// Don't set controller reference for shared ClusterRole
	return nil
}

func (authz *FeastAuthorization) initFeastClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: authz.getFeastClusterRoleBindingName()},
	}
	clusterRoleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"))
	return clusterRoleBinding
}

func (authz *FeastAuthorization) setFeastClusterRoleBinding(clusterRoleBinding *rbacv1.ClusterRoleBinding) error {
	clusterRoleBinding.Labels = authz.getLabels()
	clusterRoleBinding.Subjects = []rbacv1.Subject{
		{
			Kind:      "ServiceAccount",
			Name:      authz.getFeastServiceAccountName(),
			Namespace: authz.Handler.FeatureStore.Namespace,
		},
	}
	clusterRoleBinding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "ClusterRole",
		Name:     authz.getFeastClusterRoleName(),
	}
	return nil
}

// Create ClusterRoleBinding
func (authz *FeastAuthorization) createFeastClusterRoleBinding() error {
	logger := log.FromContext(authz.Handler.Context)
	clusterRoleBinding := authz.initFeastClusterRoleBinding()
	if op, err := controllerutil.CreateOrUpdate(authz.Handler.Context, authz.Handler.Client, clusterRoleBinding, controllerutil.MutateFn(func() error {
		return authz.setFeastClusterRoleBinding(clusterRoleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ClusterRoleBinding", clusterRoleBinding.Name, "operation", op)
	}

	return nil
}

func (authz *FeastAuthorization) initFeastRole() *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: authz.getFeastRoleName(), Namespace: authz.Handler.FeatureStore.Namespace},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	return role
}

func (authz *FeastAuthorization) setFeastRole(role *rbacv1.Role) error {
	role.Labels = authz.getLabels()
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"roles", "rolebindings"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"tokenreviews"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"subjectaccessreviews"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"namespaces"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"clusterroles"},
			Verbs:     []string{"get", "list"},
		},
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"clusterrolebindings"},
			Verbs:     []string{"get", "list"},
		},
	}

	return controllerutil.SetControllerReference(authz.Handler.FeatureStore, role, authz.Handler.Scheme)
}

func (authz *FeastAuthorization) createFeastRoleBinding() error {
	logger := log.FromContext(authz.Handler.Context)
	roleBinding := authz.initFeastRoleBinding()
	if op, err := controllerutil.CreateOrUpdate(authz.Handler.Context, authz.Handler.Client, roleBinding, controllerutil.MutateFn(func() error {
		return authz.setFeastRoleBinding(roleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "RoleBinding", roleBinding.Name, "operation", op)
	}

	return nil
}

func (authz *FeastAuthorization) initFeastRoleBinding() *rbacv1.RoleBinding {
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: authz.getFeastRoleName(), Namespace: authz.Handler.FeatureStore.Namespace},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	return roleBinding
}

func (authz *FeastAuthorization) setFeastRoleBinding(roleBinding *rbacv1.RoleBinding) error {
	roleBinding.Labels = authz.getLabels()
	roleBinding.Subjects = []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      services.GetFeastName(authz.Handler.FeatureStore),
		Namespace: authz.Handler.FeatureStore.Namespace,
	}}
	roleBinding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     authz.getFeastRoleName(),
	}

	return controllerutil.SetControllerReference(authz.Handler.FeatureStore, roleBinding, authz.Handler.Scheme)
}

func (authz *FeastAuthorization) createAuthRole(roleName string) error {
	logger := log.FromContext(authz.Handler.Context)
	role := authz.initAuthRole(roleName)
	if op, err := controllerutil.CreateOrUpdate(authz.Handler.Context, authz.Handler.Client, role, controllerutil.MutateFn(func() error {
		return authz.setAuthRole(role)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}

	return nil
}

func (authz *FeastAuthorization) initAuthRole(roleName string) *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: authz.Handler.FeatureStore.Namespace},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	return role
}

func (authz *FeastAuthorization) setAuthRole(role *rbacv1.Role) error {
	role.Labels = authz.getLabels()
	role.Rules = []rbacv1.PolicyRule{}

	return controllerutil.SetControllerReference(authz.Handler.FeatureStore, role, authz.Handler.Scheme)
}

func (authz *FeastAuthorization) getLabels() map[string]string {
	return map[string]string{
		services.NameLabelKey:        authz.Handler.FeatureStore.Name,
		services.ServiceTypeLabelKey: string(services.AuthzFeastType),
	}
}

func (authz *FeastAuthorization) setFeastKubernetesAuthCondition(err error) error {
	if err != nil {
		logger := log.FromContext(authz.Handler.Context)
		cond := feastKubernetesAuthConditions[metav1.ConditionFalse]
		cond.Message = "Error: " + err.Error()
		apimeta.SetStatusCondition(&authz.Handler.FeatureStore.Status.Conditions, cond)
		logger.Error(err, "Error deploying the Kubernetes authorization")
		return err
	} else {
		apimeta.SetStatusCondition(&authz.Handler.FeatureStore.Status.Conditions, feastKubernetesAuthConditions[metav1.ConditionTrue])
	}
	return nil
}

func (authz *FeastAuthorization) getFeastRoleName() string {
	return GetFeastRoleName(authz.Handler.FeatureStore)
}

func GetFeastRoleName(featureStore *feastdevv1alpha1.FeatureStore) string {
	return services.GetFeastName(featureStore)
}

func (authz *FeastAuthorization) getFeastClusterRoleName() string {
	return GetFeastClusterRoleName(authz.Handler.FeatureStore)
}

func GetFeastClusterRoleName(featureStore *feastdevv1alpha1.FeatureStore) string {
	// Use a shared ClusterRole name for all Feast instances
	// This allows multiple FeatureStores to share the same Token Access Review permissions
	return "feast-token-review-cluster-role"
}

func (authz *FeastAuthorization) getFeastClusterRoleBindingName() string {
	return GetFeastClusterRoleBindingName(authz.Handler.FeatureStore)
}

func GetFeastClusterRoleBindingName(featureStore *feastdevv1alpha1.FeatureStore) string {
	return services.GetFeastName(featureStore) + "-cluster-binding"
}

func (authz *FeastAuthorization) getFeastServiceAccountName() string {
	return services.GetFeastName(authz.Handler.FeatureStore)
}

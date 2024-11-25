package auth

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
func (auth *FeastAuth) Deploy() error {
	authConfig := auth.Handler.FeatureStore.Status.Applied.AuthConfig
	if authConfig != nil {
		if authConfig.KubernetesAuth != nil {
			if err := auth.deployKubernetesAuth(authConfig.KubernetesAuth); err != nil {
				return err
			}
		} else {
			auth.removeOrphanedRoles()
			_ = auth.Handler.DeleteOwnedFeastObj(auth.initFeastRole())
			_ = auth.Handler.DeleteOwnedFeastObj(auth.initFeastRoleBinding())
		}
	}
	return nil
}

func (auth *FeastAuth) deployKubernetesAuth(kubernetesAuth *feastdevv1alpha1.KubernetesAuth) error {
	auth.removeOrphanedRoles()

	if err := auth.createFeastRole(); err != nil {
		return auth.setFeastKubernetesAuthCondition(err)
	}
	if err := auth.createFeastRoleBinding(); err != nil {
		return auth.setFeastKubernetesAuthCondition(err)
	}

	for _, roleName := range kubernetesAuth.Roles {
		if err := auth.createAuthRole(roleName); err != nil {
			return auth.setFeastKubernetesAuthCondition(err)
		}
	}
	return auth.setFeastKubernetesAuthCondition(nil)
}

func (auth *FeastAuth) removeOrphanedRoles() {
	roleList := &rbacv1.RoleList{}
	err := auth.Handler.Client.List(context.TODO(), roleList, &client.ListOptions{
		Namespace:     auth.Handler.FeatureStore.Namespace,
		LabelSelector: labels.SelectorFromSet(auth.getLabels()),
	})
	if err != nil {
		return
	}

	desiredRoles := []string{}
	if auth.Handler.FeatureStore.Status.Applied.AuthConfig.KubernetesAuth != nil {
		desiredRoles = auth.Handler.FeatureStore.Status.Applied.AuthConfig.KubernetesAuth.Roles
	}
	for _, role := range roleList.Items {
		roleName := role.Name
		if roleName != auth.getFeastRoleName() && !slices.Contains(desiredRoles, roleName) {
			_ = auth.Handler.DeleteOwnedFeastObj(auth.initAuthRole(roleName))
		}
	}
}

func (auth *FeastAuth) createFeastRole() error {
	logger := log.FromContext(auth.Handler.Context)
	role := auth.initFeastRole()
	if op, err := controllerutil.CreateOrUpdate(auth.Handler.Context, auth.Handler.Client, role, controllerutil.MutateFn(func() error {
		return auth.setFeastRole(role)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}

	return nil
}

func (auth *FeastAuth) initFeastRole() *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: auth.getFeastRoleName(), Namespace: auth.Handler.FeatureStore.Namespace},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	return role
}

func (auth *FeastAuth) setFeastRole(role *rbacv1.Role) error {
	role.Labels = auth.getLabels()
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups: []string{rbacv1.GroupName},
			Resources: []string{"roles", "rolebindings"},
			Verbs:     []string{"get", "list", "watch"},
		},
	}

	return controllerutil.SetControllerReference(auth.Handler.FeatureStore, role, auth.Handler.Scheme)
}

func (auth *FeastAuth) createFeastRoleBinding() error {
	logger := log.FromContext(auth.Handler.Context)
	roleBinding := auth.initFeastRoleBinding()
	if op, err := controllerutil.CreateOrUpdate(auth.Handler.Context, auth.Handler.Client, roleBinding, controllerutil.MutateFn(func() error {
		return auth.setFeastRoleBinding(roleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "RoleBinding", roleBinding.Name, "operation", op)
	}

	return nil
}

func (auth *FeastAuth) initFeastRoleBinding() *rbacv1.RoleBinding {
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: auth.getFeastRoleName(), Namespace: auth.Handler.FeatureStore.Namespace},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	return roleBinding
}

func (auth *FeastAuth) setFeastRoleBinding(roleBinding *rbacv1.RoleBinding) error {
	roleBinding.Labels = auth.getLabels()
	roleBinding.Subjects = []rbacv1.Subject{}
	if auth.Handler.FeatureStore.Status.Applied.Services.OfflineStore != nil {
		roleBinding.Subjects = append(roleBinding.Subjects, rbacv1.Subject{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      services.GetFeastServiceName(auth.Handler.FeatureStore, services.OfflineFeastType),
			Namespace: auth.Handler.FeatureStore.Namespace,
		})
	}
	if auth.Handler.FeatureStore.Status.Applied.Services.OnlineStore != nil {
		roleBinding.Subjects = append(roleBinding.Subjects, rbacv1.Subject{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      services.GetFeastServiceName(auth.Handler.FeatureStore, services.OnlineFeastType),
			Namespace: auth.Handler.FeatureStore.Namespace,
		})
	}
	if services.IsLocalRegistry(auth.Handler.FeatureStore) {
		roleBinding.Subjects = append(roleBinding.Subjects, rbacv1.Subject{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      services.GetFeastServiceName(auth.Handler.FeatureStore, services.RegistryFeastType),
			Namespace: auth.Handler.FeatureStore.Namespace,
		})
	}
	roleBinding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     auth.getFeastRoleName(),
	}

	return controllerutil.SetControllerReference(auth.Handler.FeatureStore, roleBinding, auth.Handler.Scheme)
}

func (auth *FeastAuth) createAuthRole(roleName string) error {
	logger := log.FromContext(auth.Handler.Context)
	role := auth.initAuthRole(roleName)
	if op, err := controllerutil.CreateOrUpdate(auth.Handler.Context, auth.Handler.Client, role, controllerutil.MutateFn(func() error {
		return auth.setAuthRole(role)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}

	return nil
}

func (auth *FeastAuth) initAuthRole(roleName string) *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: auth.Handler.FeatureStore.Namespace},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))
	return role
}

func (auth *FeastAuth) setAuthRole(role *rbacv1.Role) error {
	role.Labels = auth.getLabels()
	role.Rules = []rbacv1.PolicyRule{}

	return controllerutil.SetControllerReference(auth.Handler.FeatureStore, role, auth.Handler.Scheme)
}

func (auth *FeastAuth) getLabels() map[string]string {
	return map[string]string{
		services.NameLabelKey: auth.Handler.FeatureStore.Name,
	}
}

func (auth *FeastAuth) setFeastKubernetesAuthCondition(err error) error {
	if err != nil {
		logger := log.FromContext(auth.Handler.Context)
		cond := feastKubernetesAuthConditions[metav1.ConditionFalse]
		cond.Message = "Error: " + err.Error()
		apimeta.SetStatusCondition(&auth.Handler.FeatureStore.Status.Conditions, cond)
		logger.Error(err, "Error deploying the Kubernetes authorization")
		return err
	} else {
		apimeta.SetStatusCondition(&auth.Handler.FeatureStore.Status.Conditions, feastKubernetesAuthConditions[metav1.ConditionTrue])
	}
	return nil
}

func (auth *FeastAuth) getFeastRoleName() string {
	return GetFeastRoleName(auth.Handler.FeatureStore)
}

func GetFeastRoleName(featureStore *feastdevv1alpha1.FeatureStore) string {
	return services.GetFeastName(featureStore)
}

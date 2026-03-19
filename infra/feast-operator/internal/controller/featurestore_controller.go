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

package controller

import (
	"context"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	handler "sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/access"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/authz"
	feasthandler "github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/registry"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	routev1 "github.com/openshift/api/route/v1"
)

// Constants for requeue
const (
	RequeueDelayError       = 5 * time.Second
	RequeuePeriodicInterval = 30 * time.Second
)

// FeatureStoreReconciler reconciles a FeatureStore object
type FeatureStoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=feast.dev,resources=featurestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=feast.dev,resources=featurestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=feast.dev,resources=featurestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;create;update;watch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps;persistentvolumeclaims;serviceaccounts,verbs=get;list;create;update;watch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings;subjectaccessreviews,verbs=get;list;create;update;watch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=core,resources=secrets;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;create;update;watch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *FeatureStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, recErr error) {
	logger := log.FromContext(ctx)

	// Get the FeatureStore using v1 (storage version)
	cr := &feastdevv1.FeatureStore{}
	err := r.Get(ctx, req.NamespacedName, cr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("FeatureStore CR not found, has been deleted")
			r.cleanupOnDeletion(ctx, req.NamespacedName.Namespace, req.NamespacedName.Name)
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Unable to get FeatureStore CR")
		return ctrl.Result{}, err
	}
	currentStatus := cr.Status.DeepCopy()

	if cr.DeletionTimestamp != nil {
		r.cleanupOnDeletion(ctx, cr.Namespace, cr.Name)
		return ctrl.Result{}, nil
	}

	result, recErr = r.deployFeast(ctx, cr)
	if cr.DeletionTimestamp == nil && !reflect.DeepEqual(currentStatus, cr.Status) {
		if err = r.Client.Status().Update(ctx, cr); err != nil {
			if apierrors.IsConflict(err) {
				logger.Info("FeatureStore object modified, retry syncing status")
				// Re-queue and preserve existing recErr
				result = ctrl.Result{Requeue: true, RequeueAfter: RequeueDelayError}
			}
			logger.Error(err, "Error updating the FeatureStore status")
			if recErr == nil {
				// There is no existing recErr. Set it to the status update error
				recErr = err
			}
		}
	}

	if recErr == nil && cr.DeletionTimestamp == nil && apimeta.IsStatusConditionTrue(cr.Status.Conditions, feastdevv1.ReadyType) {
		if err := access.EnsureNamespaceLabel(ctx, r.Client, cr.Namespace); err != nil {
			logger.Error(err, "Failed to add Feast label to namespace")
		}
		r.addToNamespaceRegistry(ctx, cr)
		policies, err := r.fetchPermissionsFromRegistry(ctx, cr)
		if err != nil {
			logger.Error(err, "Failed to fetch permissions from registry")
		}
		if err != nil || len(policies) == 0 || cr.Status.ClientConfigMap == "" {
			logger.V(1).Info("Auto-access prerequisites missing or registry unreachable; cleaning up stale auto-access RBAC",
				"policies", len(policies),
				"clientConfigMapSet", cr.Status.ClientConfigMap != "",
				"fetchError", err != nil,
			)
			if err := access.CleanupAutoAccessRBAC(ctx, r.Client, cr.Namespace, cr.Name); err != nil {
				logger.Error(err, "Failed to cleanup stale auto-access RBAC")
			}
		} else {
			if err := access.ReconcileAutoAccessRBAC(ctx, r.Client, r.Scheme, cr, cr.Namespace, cr.Name, cr.Status.ClientConfigMap, policies); err != nil {
				logger.Error(err, "Failed to reconcile auto-access RBAC")
			}
		}
	}

	if recErr == nil && result.RequeueAfter == 0 {
		result.RequeueAfter = RequeuePeriodicInterval
	}
	return result, recErr
}

func (r *FeatureStoreReconciler) fetchPermissionsFromRegistry(ctx context.Context, cr *feastdevv1.FeatureStore) ([]registry.PermissionPolicy, error) {
	logger := log.FromContext(ctx)
	registryRest := cr.Status.ServiceHostnames.RegistryRest
	if registryRest == "" {
		logger.V(1).Info("Skipping permission fetch: registry REST API hostname is not set (ensure RestAPI is enabled)")
		return nil, nil
	}
	project := cr.Status.Applied.FeastProject
	if project == "" {
		project = cr.Spec.FeastProject
	}
	if project == "" {
		logger.Info("Skipping permission fetch: feast project name is not set")
		return nil, nil
	}
	intraCommToken, err := r.readIntraCommunicationToken(ctx, cr)
	if err != nil {
		return nil, err
	}
	useTLS := cr.Status.Applied.Services.Registry != nil &&
		cr.Status.Applied.Services.Registry.Local != nil &&
		cr.Status.Applied.Services.Registry.Local.Server != nil &&
		cr.Status.Applied.Services.Registry.Local.Server.TLS.IsTLS()
	return registry.ListPermissions(ctx, registryRest, project, intraCommToken, useTLS)
}

func (r *FeatureStoreReconciler) readIntraCommunicationToken(ctx context.Context, cr *feastdevv1.FeatureStore) (string, error) {
	cmName := services.GetIntraCommunicationConfigMapName(cr.Name)
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: cr.Namespace}, cm); err != nil {
		return "", err
	}
	return cm.Data["token"], nil
}

func (r *FeatureStoreReconciler) addToNamespaceRegistry(ctx context.Context, cr *feastdevv1.FeatureStore) {
	logger := log.FromContext(ctx)
	feast := services.FeastServices{
		Handler: feasthandler.FeastHandler{
			Client:       r.Client,
			Context:      ctx,
			FeatureStore: cr,
			Scheme:       r.Scheme,
		},
	}
	if err := feast.AddToNamespaceRegistry(); err != nil {
		logger.Error(err, "Failed to add feature store to namespace registry")
	}
}

func (r *FeatureStoreReconciler) cleanupOnDeletion(ctx context.Context, namespace, name string) {
	logger := log.FromContext(ctx)
	otherCount := r.countOtherFeatureStoresInNamespace(ctx, namespace, name)
	if err := access.RemoveNamespaceLabelIfLast(ctx, r.Client, namespace, otherCount); err != nil {
		logger.Error(err, "Failed to remove Feast label from namespace")
	}
	if err := access.CleanupAutoAccessRBAC(ctx, r.Client, namespace, name); err != nil {
		logger.Error(err, "Failed to cleanup auto-access RBAC")
	}
	clusterCount := r.countOtherFeatureStoresInCluster(ctx, namespace, name)
	if err := access.CleanupDiscoverClusterRoleIfLast(ctx, r.Client, clusterCount); err != nil {
		logger.Error(err, "Failed to cleanup discover ClusterRole")
	}
	r.cleanupNamespaceRegistry(ctx, &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	})
}

func (r *FeatureStoreReconciler) cleanupNamespaceRegistry(ctx context.Context, cr *feastdevv1.FeatureStore) {
	logger := log.FromContext(ctx)
	feast := services.FeastServices{
		Handler: feasthandler.FeastHandler{
			Client:       r.Client,
			Context:      ctx,
			FeatureStore: cr,
			Scheme:       r.Scheme,
		},
	}
	if err := feast.RemoveFromNamespaceRegistry(); err != nil {
		logger.Error(err, "Failed to remove feature store from namespace registry")
	}
}

func (r *FeatureStoreReconciler) countOtherFeatureStoresInNamespace(ctx context.Context, namespace, excludeName string) int {
	var list feastdevv1.FeatureStoreList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return -1
	}
	count := 0
	for i := range list.Items {
		if list.Items[i].Name != excludeName && list.Items[i].DeletionTimestamp == nil {
			count++
		}
	}
	return count
}

func (r *FeatureStoreReconciler) countOtherFeatureStoresInCluster(ctx context.Context, namespace, excludeName string) int {
	var list feastdevv1.FeatureStoreList
	if err := r.List(ctx, &list); err != nil {
		return -1
	}
	count := 0
	for i := range list.Items {
		item := &list.Items[i]
		if (item.Name != excludeName || item.Namespace != namespace) && item.DeletionTimestamp == nil {
			count++
		}
	}
	return count
}

func (r *FeatureStoreReconciler) deployFeast(ctx context.Context, cr *feastdevv1.FeatureStore) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx)
	condition := metav1.Condition{
		Type:    feastdevv1.ReadyType,
		Status:  metav1.ConditionTrue,
		Reason:  feastdevv1.ReadyReason,
		Message: feastdevv1.ReadyMessage,
	}
	feast := services.FeastServices{
		Handler: feasthandler.FeastHandler{
			Client:       r.Client,
			Context:      ctx,
			FeatureStore: cr,
			Scheme:       r.Scheme,
		},
	}
	authz := authz.FeastAuthorization{
		Handler: feast.Handler,
	}

	// status defaults must be applied before deployments
	errResult := ctrl.Result{Requeue: true, RequeueAfter: RequeueDelayError}
	if err = feast.ApplyDefaults(); err != nil {
		result = errResult
	} else if err = authz.Deploy(); err != nil {
		result = errResult
	} else if err = feast.Deploy(); err != nil {
		result = errResult
	}
	if err != nil {
		condition = metav1.Condition{
			Type:    feastdevv1.ReadyType,
			Status:  metav1.ConditionFalse,
			Reason:  feastdevv1.FailedReason,
			Message: "Error: " + err.Error(),
		}
	} else {
		deployment, deploymentErr := feast.GetDeployment()
		if deploymentErr != nil {
			condition = metav1.Condition{
				Type:    feastdevv1.ReadyType,
				Status:  metav1.ConditionUnknown,
				Reason:  feastdevv1.DeploymentNotAvailableReason,
				Message: feastdevv1.DeploymentNotAvailableMessage,
			}

			result = errResult
		} else {
			isDeployAvailable := services.IsDeploymentAvailable(deployment.Status.Conditions)
			if !isDeployAvailable {
				msg := feastdevv1.DeploymentNotAvailableMessage
				if podMsg := feast.GetPodContainerFailureMessage(deployment); podMsg != "" {
					msg = msg + ": " + podMsg
				}
				condition = metav1.Condition{
					Type:    feastdevv1.ReadyType,
					Status:  metav1.ConditionUnknown,
					Reason:  feastdevv1.DeploymentNotAvailableReason,
					Message: msg,
				}

				result = errResult
			}
		}
	}

	logger.Info(condition.Message)
	apimeta.SetStatusCondition(&cr.Status.Conditions, condition)
	if apimeta.IsStatusConditionTrue(cr.Status.Conditions, feastdevv1.ReadyType) {
		cr.Status.Phase = feastdevv1.ReadyPhase
	} else if apimeta.IsStatusConditionFalse(cr.Status.Conditions, feastdevv1.ReadyType) {
		cr.Status.Phase = feastdevv1.FailedPhase
	} else {
		cr.Status.Phase = feastdevv1.PendingPhase
	}

	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *FeatureStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&feastdevv1.FeatureStore{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&rbacv1.Role{}).
		Owns(&batchv1.CronJob{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&feastdevv1.FeatureStore{}, handler.EnqueueRequestsFromMapFunc(r.mapFeastRefsToFeastRequests))

	if services.IsOpenShift() {
		bldr = bldr.Owns(&routev1.Route{})
	}

	return bldr.Complete(r)

}

// if a remotely referenced FeatureStore is changed, reconcile any FeatureStores that reference it.
func (r *FeatureStoreReconciler) mapFeastRefsToFeastRequests(ctx context.Context, object client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)

	feastRef, ok := object.(*feastdevv1.FeatureStore)
	if !ok {
		logger.Error(nil, "Unexpected object type in mapFeastRefsToFeastRequests")
		return nil
	}

	// list all FeatureStores in the cluster
	var feastList feastdevv1.FeatureStoreList
	if err := r.List(ctx, &feastList, client.InNamespace("")); err != nil {
		logger.Error(err, "could not list FeatureStores. "+
			"FeatureStores affected by changes to the referenced FeatureStore object will not be reconciled.")
		return nil
	}

	feastRefNsName := client.ObjectKeyFromObject(feastRef)
	var requests []reconcile.Request
	for _, obj := range feastList.Items {
		objNsName := client.ObjectKeyFromObject(&obj)
		// this if statement is extra protection against any potential infinite reconcile loops
		if feastRefNsName != objNsName {
			feast := services.FeastServices{
				Handler: feasthandler.FeastHandler{
					Client:       r.Client,
					Context:      ctx,
					FeatureStore: &obj,
					Scheme:       r.Scheme,
				}}
			if feast.IsRemoteRefRegistry() {
				remoteRef := obj.Status.Applied.Services.Registry.Remote.FeastRef
				remoteRefNsName := types.NamespacedName{Name: remoteRef.Name, Namespace: remoteRef.Namespace}
				if feastRefNsName == remoteRefNsName {
					requests = append(requests, reconcile.Request{NamespacedName: objNsName})
				}
			}
		}
	}

	return requests
}

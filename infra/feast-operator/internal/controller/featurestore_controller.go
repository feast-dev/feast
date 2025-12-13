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
	"encoding/json"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/authz"
	feasthandler "github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	routev1 "github.com/openshift/api/route/v1"
)

// Constants for requeue
const (
	RequeueDelayError = 5 * time.Second
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
// +kubebuilder:rbac:groups=core,resources=secrets;pods;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;create;update;watch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

// convertV1ToV1Alpha1 converts a v1 FeatureStore to v1alpha1 for internal use
// Since both types have identical structures, we use JSON marshaling/unmarshaling
func convertV1ToV1Alpha1(v1Obj *feastdevv1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	// Use JSON marshaling/unmarshaling since both types have identical JSON structure
	v1alpha1Obj := &feastdevv1alpha1.FeatureStore{
		ObjectMeta: v1Obj.ObjectMeta,
	}

	// Copy spec and status using JSON as intermediate format
	specData, err := json.Marshal(v1Obj.Spec)
	if err != nil {
		// If marshaling fails, return object with just metadata
		return v1alpha1Obj
	}
	if err := json.Unmarshal(specData, &v1alpha1Obj.Spec); err != nil {
		// If unmarshaling fails, return object with just metadata
		return v1alpha1Obj
	}
	statusData, err := json.Marshal(v1Obj.Status)
	if err != nil {
		// If marshaling fails, return object with spec but no status
		return v1alpha1Obj
	}
	if err := json.Unmarshal(statusData, &v1alpha1Obj.Status); err != nil {
		// If unmarshaling fails, return object with spec but no status
		return v1alpha1Obj
	}

	return v1alpha1Obj
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *FeatureStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, recErr error) {
	logger := log.FromContext(ctx)

	// Try to get as v1 first (storage version), then fall back to v1alpha1
	var cr *feastdevv1alpha1.FeatureStore
	var originalV1Obj *feastdevv1.FeatureStore
	v1Obj := &feastdevv1.FeatureStore{}
	err := r.Get(ctx, req.NamespacedName, v1Obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// CR deleted since request queued, child objects getting GC'd, no requeue
			logger.V(1).Info("FeatureStore CR not found, has been deleted")
			// Clean up namespace registry entry even if the CR is not found
			if err := r.cleanupNamespaceRegistry(ctx, &feastdevv1alpha1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      req.NamespacedName.Name,
					Namespace: req.NamespacedName.Namespace,
				},
			}); err != nil {
				logger.Error(err, "Failed to clean up namespace registry entry for deleted FeatureStore")
				// Don't return error here as the CR is already deleted
			}
			return ctrl.Result{}, nil
		}
		// Try v1alpha1 if v1 fails
		v1alpha1Obj := &feastdevv1alpha1.FeatureStore{}
		err = r.Get(ctx, req.NamespacedName, v1alpha1Obj)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// CR deleted since request queued, child objects getting GC'd, no requeue
				logger.V(1).Info("FeatureStore CR not found, has been deleted")
				// Clean up namespace registry entry even if the CR is not found
				if err := r.cleanupNamespaceRegistry(ctx, &feastdevv1alpha1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      req.NamespacedName.Name,
						Namespace: req.NamespacedName.Namespace,
					},
				}); err != nil {
					logger.Error(err, "Failed to clean up namespace registry entry for deleted FeatureStore")
					// Don't return error here as the CR is already deleted
				}
				return ctrl.Result{}, nil
			}
			logger.Error(err, "Unable to get FeatureStore CR")
			return ctrl.Result{}, err
		}
		cr = v1alpha1Obj
	} else {
		// Convert v1 to v1alpha1 for internal use
		originalV1Obj = v1Obj
		cr = convertV1ToV1Alpha1(v1Obj)
	}
	currentStatus := cr.Status.DeepCopy()

	// Handle deletion - clean up namespace registry entry
	if cr.DeletionTimestamp != nil {
		logger.Info("FeatureStore is being deleted, cleaning up namespace registry entry")
		if err := r.cleanupNamespaceRegistry(ctx, cr); err != nil {
			logger.Error(err, "Failed to clean up namespace registry entry")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	result, recErr = r.deployFeast(ctx, cr)
	if cr.DeletionTimestamp == nil && !reflect.DeepEqual(currentStatus, cr.Status) {
		// Update status - need to update in the original version (v1 if it was v1, v1alpha1 if it was v1alpha1)
		var statusObj client.Object
		if originalV1Obj != nil {
			// Convert back to v1 for status update
			originalV1Obj.Status = feastdevv1.FeatureStoreStatus{}
			statusData, err := json.Marshal(cr.Status)
			if err != nil {
				logger.Error(err, "Failed to marshal status for v1 conversion")
				statusObj = cr
			} else {
				if err := json.Unmarshal(statusData, &originalV1Obj.Status); err != nil {
					logger.Error(err, "Failed to unmarshal status for v1 conversion")
					statusObj = cr
				} else {
					statusObj = originalV1Obj
				}
			}
		} else {
			statusObj = cr
		}
		if err = r.Client.Status().Update(ctx, statusObj); err != nil {
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

	// Add to namespace registry if deployment was successful and not being deleted
	if recErr == nil && cr.DeletionTimestamp == nil {
		feast := services.FeastServices{
			Handler: feasthandler.FeastHandler{
				Client:       r.Client,
				Context:      ctx,
				FeatureStore: cr,
				Scheme:       r.Scheme,
			},
		}
		if err := feast.AddToNamespaceRegistry(); err != nil {
			logger.Error(err, "Failed to add FeatureStore to namespace registry")
			// Don't return error here as the FeatureStore is already deployed successfully
		}
	}

	return result, recErr
}

func (r *FeatureStoreReconciler) deployFeast(ctx context.Context, cr *feastdevv1alpha1.FeatureStore) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx)
	condition := metav1.Condition{
		Type:    feastdevv1alpha1.ReadyType,
		Status:  metav1.ConditionTrue,
		Reason:  feastdevv1alpha1.ReadyReason,
		Message: feastdevv1alpha1.ReadyMessage,
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
			Type:    feastdevv1alpha1.ReadyType,
			Status:  metav1.ConditionFalse,
			Reason:  feastdevv1alpha1.FailedReason,
			Message: "Error: " + err.Error(),
		}
	} else {
		deployment, deploymentErr := feast.GetDeployment()
		if deploymentErr != nil {
			condition = metav1.Condition{
				Type:    feastdevv1alpha1.ReadyType,
				Status:  metav1.ConditionUnknown,
				Reason:  feastdevv1alpha1.DeploymentNotAvailableReason,
				Message: feastdevv1alpha1.DeploymentNotAvailableMessage,
			}

			result = errResult
		} else {
			isDeployAvailable := services.IsDeploymentAvailable(deployment.Status.Conditions)
			if !isDeployAvailable {
				condition = metav1.Condition{
					Type:    feastdevv1alpha1.ReadyType,
					Status:  metav1.ConditionUnknown,
					Reason:  feastdevv1alpha1.DeploymentNotAvailableReason,
					Message: feastdevv1alpha1.DeploymentNotAvailableMessage,
				}

				result = errResult
			}
		}
	}

	logger.Info(condition.Message)
	apimeta.SetStatusCondition(&cr.Status.Conditions, condition)
	if apimeta.IsStatusConditionTrue(cr.Status.Conditions, feastdevv1alpha1.ReadyType) {
		cr.Status.Phase = feastdevv1alpha1.ReadyPhase
	} else if apimeta.IsStatusConditionFalse(cr.Status.Conditions, feastdevv1alpha1.ReadyType) {
		cr.Status.Phase = feastdevv1alpha1.FailedPhase
	} else {
		cr.Status.Phase = feastdevv1alpha1.PendingPhase
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
		Watches(&feastdevv1.FeatureStore{}, handler.EnqueueRequestsFromMapFunc(r.mapFeastRefsToFeastRequests)).
		Watches(&feastdevv1alpha1.FeatureStore{}, handler.EnqueueRequestsFromMapFunc(r.mapFeastRefsToFeastRequests))

	// Also watch v1alpha1 for backwards compatibility
	bldr = bldr.Watches(&feastdevv1alpha1.FeatureStore{}, &handler.EnqueueRequestForObject{})

	if services.IsOpenShift() {
		bldr = bldr.Owns(&routev1.Route{})
	}

	return bldr.Complete(r)

}

// cleanupNamespaceRegistry removes the feature store instance from the namespace registry
func (r *FeatureStoreReconciler) cleanupNamespaceRegistry(ctx context.Context, cr *feastdevv1alpha1.FeatureStore) error {
	feast := services.FeastServices{
		Handler: feasthandler.FeastHandler{
			Client:       r.Client,
			Context:      ctx,
			FeatureStore: cr,
			Scheme:       r.Scheme,
		},
	}

	return feast.RemoveFromNamespaceRegistry()
}

// if a remotely referenced FeatureStore is changed, reconcile any FeatureStores that reference it.
func (r *FeatureStoreReconciler) mapFeastRefsToFeastRequests(ctx context.Context, object client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)

	// Handle both v1 and v1alpha1 versions
	var feastRef *feastdevv1alpha1.FeatureStore
	switch obj := object.(type) {
	case *feastdevv1.FeatureStore:
		feastRef = convertV1ToV1Alpha1(obj)
	case *feastdevv1alpha1.FeatureStore:
		feastRef = obj
	default:
		logger.Error(nil, "Unexpected object type in mapFeastRefsToFeastRequests")
		return nil
	}

	// list all FeatureStores in the cluster
	var feastList feastdevv1alpha1.FeatureStoreList
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

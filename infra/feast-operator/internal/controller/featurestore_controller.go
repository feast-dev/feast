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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
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

//+kubebuilder:rbac:groups=feast.dev,resources=featurestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=feast.dev,resources=featurestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=feast.dev,resources=featurestores/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;create;update;watch;delete
//+kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;create;update;watch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *FeatureStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, recErr error) {
	logger := log.FromContext(ctx)

	cr := &feastdevv1alpha1.FeatureStore{}
	err := r.Get(ctx, req.NamespacedName, cr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// CR deleted since request queued, child objects getting GC'd, no requeue
			logger.V(1).Info("FeatureStore CR not found, has been deleted")
			return ctrl.Result{}, nil
		}
		// error fetching FeatureStore instance, requeue and try again
		logger.Error(err, "Unable to get FeatureStore CR")
		return ctrl.Result{}, err
	}
	currentStatus := cr.Status.DeepCopy()

	// initial status defaults must occur before feast deployment
	applyDefaultsToStatus(cr)
	result, recErr = r.deployFeast(ctx, cr)
	if cr.DeletionTimestamp == nil && !reflect.DeepEqual(currentStatus, cr.Status) {
		if err := r.Client.Status().Update(ctx, cr); err != nil {
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
		Client:       r.Client,
		Context:      ctx,
		FeatureStore: cr,
		Scheme:       r.Scheme,
	}
	if err = feast.Deploy(); err != nil {
		condition = metav1.Condition{
			Type:    feastdevv1alpha1.ReadyType,
			Status:  metav1.ConditionFalse,
			Reason:  feastdevv1alpha1.FailedReason,
			Message: "Error: " + err.Error(),
		}
		result = ctrl.Result{Requeue: true, RequeueAfter: RequeueDelayError}
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&feastdevv1alpha1.FeatureStore{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(&feastdevv1alpha1.FeatureStore{}, handler.EnqueueRequestsFromMapFunc(r.mapFeastRefsToFeastRequests)).
		Complete(r)
}

// if a remotely referenced FeatureStore is changed, reconcile any FeatureStores that reference it.
func (r *FeatureStoreReconciler) mapFeastRefsToFeastRequests(ctx context.Context, object client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)
	feastRef := object.(*feastdevv1alpha1.FeatureStore)

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
				Client:       r.Client,
				Context:      ctx,
				FeatureStore: &obj,
				Scheme:       r.Scheme,
			}
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

func applyDefaultsToStatus(cr *feastdevv1alpha1.FeatureStore) {
	cr.Status.FeastVersion = feastversion.FeastVersion
	applied := cr.Spec.DeepCopy()
	if applied.Services == nil {
		applied.Services = &feastdevv1alpha1.FeatureStoreServices{}
	}

	// default to registry service deployment
	if applied.Services.Registry == nil {
		applied.Services.Registry = &feastdevv1alpha1.Registry{}
	}
	// if remote registry not set, proceed w/ local registry defaults
	if applied.Services.Registry.Remote == nil {
		// if local registry not set, apply an empty pointer struct
		if applied.Services.Registry.Local == nil {
			applied.Services.Registry.Local = &feastdevv1alpha1.LocalRegistryConfig{}
		}
		setServiceDefaultConfigs(&applied.Services.Registry.Local.ServiceConfigs.DefaultConfigs)
	}
	if applied.Services.OfflineStore != nil {
		setServiceDefaultConfigs(&applied.Services.OfflineStore.ServiceConfigs.DefaultConfigs)
	}
	if applied.Services.OnlineStore != nil {
		setServiceDefaultConfigs(&applied.Services.OnlineStore.ServiceConfigs.DefaultConfigs)
	}

	// overwrite status.applied with every reconcile
	applied.DeepCopyInto(&cr.Status.Applied)
}

func setServiceDefaultConfigs(defaultConfigs *feastdevv1alpha1.DefaultConfigs) {
	if defaultConfigs.Image == nil {
		defaultConfigs.Image = &services.DefaultImage
	}
}

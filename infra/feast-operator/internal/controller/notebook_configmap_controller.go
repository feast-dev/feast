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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	handler "sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

const (
	// NotebookFeastIntegrationLabel is the label that must be set to 'true' to enable feast integration
	NotebookFeastIntegrationLabel = "opendatahub.io/feast-integration"
	// NotebookFeastConfigAnnotation is the annotation key on Notebooks that contains comma-separated feast project names
	NotebookFeastConfigAnnotation = "opendatahub.io/feast-config"
	// NotebookConfigMapNameSuffix is the suffix for ConfigMap names created for each notebook
	NotebookConfigMapNameSuffix = "-feast-config"
	// TrueValue is the value that indicates feast integration is enabled
	TrueValue = "true"
)

// NotebookConfigMapReconciler reconciles Notebooks to create/update notebook-feast-config ConfigMaps
// based on feast project annotations
type NotebookConfigMapReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// NotebookGVK is the GroupVersionKind of the Notebook custom resource
	NotebookGVK schema.GroupVersionKind
}

// +kubebuilder:rbac:groups=kubeflow.org,resources=notebooks,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=feast.dev,resources=featurestores,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list

// Reconcile handles Notebook reconciliation
func (r *NotebookConfigMapReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	// Fetch the Notebook
	notebook := &unstructured.Unstructured{}
	notebook.SetGroupVersionKind(r.NotebookGVK)
	if err := r.Get(ctx, req.NamespacedName, notebook); err != nil {
		if apierrors.IsNotFound(err) {
			// Notebook deleted - ConfigMap will be automatically deleted via owner reference
			logger.V(1).Info("Notebook not found, ConfigMap will be cleaned up by owner reference")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Unable to fetch Notebook")
		return ctrl.Result{}, err
	}

	// If Notebook is being deleted, owner reference will handle ConfigMap cleanup
	if notebook.GetDeletionTimestamp() != nil {
		logger.V(1).Info("Notebook is being deleted, ConfigMap will be cleaned up by owner reference")
		return ctrl.Result{}, nil
	}

	// Check if feast integration is enabled via label
	labels := notebook.GetLabels()
	if labels == nil {
		logger.V(1).Info("No integration label found on Notebook, skipping reconciliation")
		return r.cleanupNotebookConfigMap(ctx, req.Namespace, req.Name)
	}

	feastIntegrationEnabled, exists := labels[NotebookFeastIntegrationLabel]
	if !exists || feastIntegrationEnabled != TrueValue {
		logger.V(1).Info("Feast integration not enabled, skipping reconciliation", "label", NotebookFeastIntegrationLabel, "value", feastIntegrationEnabled)
		return r.cleanupNotebookConfigMap(ctx, req.Namespace, req.Name)
	}

	// Extract feast project names from annotation
	annotations := notebook.GetAnnotations()

	feastConfigAnnotation, exists := annotations[NotebookFeastConfigAnnotation]
	if feastIntegrationEnabled == TrueValue && (!exists || feastConfigAnnotation == "") {
		logger.V(1).Info("No feast config annotation found on Notebook, still keeping the ConfigMap for the notebook")
	}
	// Parse comma-separated project names from annotation
	projectNames := r.parseProjectNames(feastConfigAnnotation)

	logger.Info("Reconciling notebook ConfigMap", "projects", projectNames, "namespace", req.Namespace)

	// Create or update the notebook-feast-config ConfigMap
	if err := r.reconcileNotebookConfigMap(ctx, req.Namespace, notebook, projectNames); err != nil {
		logger.Error(err, "Failed to reconcile notebook ConfigMap")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// parseProjectNames parses comma-separated project names from the label value
func (r *NotebookConfigMapReconciler) parseProjectNames(labelValue string) []string {
	projects := strings.Split(labelValue, ",")
	var validProjects []string
	for _, project := range projects {
		project = strings.TrimSpace(project)
		if project != "" {
			validProjects = append(validProjects, project)
		}
	}
	return validProjects
}

// reconcileNotebookConfigMap creates or updates the notebook-feast-config ConfigMap for this notebook
func (r *NotebookConfigMapReconciler) reconcileNotebookConfigMap(
	ctx context.Context,
	namespace string,
	notebook client.Object,
	projectNames []string,
) error {
	logger := log.FromContext(ctx)

	// Each notebook gets its own ConfigMap with a unique name
	configMapName := fmt.Sprintf("%s%s", notebook.GetName(), NotebookConfigMapNameSuffix)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: namespace,
		},
	}

	if op, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		return r.setNotebookConfigMapData(ctx, configMap, notebook, projectNames)
	}); err != nil {
		return fmt.Errorf("failed to create or update notebook ConfigMap: %w", err)
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled notebook ConfigMap", "ConfigMap", configMap.Name, "operation", op)
	}

	return nil
}

// setNotebookConfigMapData sets the ConfigMap data with feast project configs for this specific notebook
func (r *NotebookConfigMapReconciler) setNotebookConfigMapData(
	ctx context.Context,
	cm *corev1.ConfigMap,
	notebook client.Object,
	projectNames []string,
) error {
	logger := log.FromContext(ctx)

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	// Track which projects should exist
	expectedProjects := make(map[string]bool)
	for _, projectName := range projectNames {
		expectedProjects[projectName] = true
	}

	// Add or update projects that are in the annotation
	// This always fetches fresh data, so any updates to FeatureStore client ConfigMaps will be picked up
	for _, projectName := range projectNames {
		clientConfigYAML, err := r.getClientConfigYAMLForProject(ctx, projectName)
		if err != nil {
			// Log error and remove this project from ConfigMap if it exists
			logger.Error(err, "Failed to get client config for project, removing from ConfigMap", "project", projectName, "notebook", notebook.GetName())
			delete(cm.Data, projectName)
			continue
		}

		// Check if this is an update
		existingYAML, exists := cm.Data[projectName]
		if exists && existingYAML != clientConfigYAML {
			logger.Info("Updating project config in ConfigMap (FeatureStore client ConfigMap changed)", "project", projectName, "notebook", notebook.GetName())
		}

		// Set or update the project config
		cm.Data[projectName] = clientConfigYAML
	}

	// Remove projects that are no longer in the annotation
	for key := range cm.Data {
		if !expectedProjects[key] {
			logger.Info("Removing project from ConfigMap (no longer in annotation)", "project", key, "notebook", notebook.GetName())
			delete(cm.Data, key)
		}
	}

	// Set labels
	if cm.Labels == nil {
		cm.Labels = make(map[string]string)
	}
	cm.Labels["managed-by"] = "feast-operator"
	cm.Labels["source-resource"] = notebook.GetName()
	cm.Labels["source-kind"] = notebook.GetObjectKind().GroupVersionKind().Kind

	// Set owner reference to the Notebook with blockOwnerDeletion: false
	// This is required because Notebook CRD doesn't support finalizers
	// The owner reference still allows Kubernetes to track the relationship
	// and we handle cleanup manually in Reconcile() when Notebook is deleted
	gvk := notebook.GetObjectKind().GroupVersionKind()
	controller := false
	blockOwnerDeletion := false
	cm.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         gvk.GroupVersion().String(),
			Kind:               gvk.Kind,
			Name:               notebook.GetName(),
			UID:                notebook.GetUID(),
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}

	return nil
}

// getClientConfigYAMLForProject finds a FeatureStore with the given project name and returns its client config YAML
func (r *NotebookConfigMapReconciler) getClientConfigYAMLForProject(ctx context.Context, projectName string) (string, error) {
	logger := log.FromContext(ctx)

	// List all FeatureStores to find one with matching feastProject
	var featureStoreList feastdevv1.FeatureStoreList
	if err := r.List(ctx, &featureStoreList, client.InNamespace("")); err != nil {
		return "", fmt.Errorf("failed to list FeatureStores: %w", err)
	}

	// Find FeatureStore with matching project name
	// Try to match by spec.feastProject first, then by metadata.name as fallback
	var matchingFeatureStore *feastdevv1.FeatureStore

	for i := range featureStoreList.Items {
		fs := &featureStoreList.Items[i]
		var matches bool

		// First, try to match by spec.feastProject
		project := fs.Spec.FeastProject
		if fs.Status.Applied.FeastProject != "" {
			project = fs.Status.Applied.FeastProject
		}
		if project == projectName {
			matches = true
		} else if fs.GetName() == projectName {
			// Fallback: match by FeatureStore metadata name
			matches = true
		}

		if matches {
			if fs.Status.ClientConfigMap != "" {
				matchingFeatureStore = fs
				break
			}
		}
	}

	if matchingFeatureStore == nil {
		// Provide helpful error message with available projects
		var availableProjects []string
		for i := range featureStoreList.Items {
			fs := &featureStoreList.Items[i]
			project := fs.Spec.FeastProject
			if fs.Status.Applied.FeastProject != "" {
				project = fs.Status.Applied.FeastProject
			}
			if project != "" {
				availableProjects = append(availableProjects, fmt.Sprintf("%s (FeatureStore: %s/%s)", project, fs.Namespace, fs.Name))
			}
		}

		if len(availableProjects) > 0 {
			return "", fmt.Errorf("no FeatureStore found with feastProject: %s. Available projects are: %v", projectName, availableProjects)
		}
		return "", fmt.Errorf("no FeatureStore found with feastProject: %s. No FeatureStores found in the cluster", projectName)
	}

	// Get the client configmap name from status
	clientConfigMapName := matchingFeatureStore.Status.ClientConfigMap
	if clientConfigMapName == "" {
		featureStoreKey := fmt.Sprintf("%s/%s", matchingFeatureStore.Namespace, matchingFeatureStore.Name)
		logger.Error(nil, "FeatureStore found but has no ClientConfigMap", "featurestore", featureStoreKey)
		return "", fmt.Errorf("FeatureStore %s/%s does not have a client ConfigMap (may still be deploying)", matchingFeatureStore.Namespace, matchingFeatureStore.Name)
	}

	// Fetch the client configmap
	clientConfigMap := &corev1.ConfigMap{}
	clientConfigMapKey := types.NamespacedName{
		Name:      clientConfigMapName,
		Namespace: matchingFeatureStore.Namespace,
	}

	if err := r.Get(ctx, clientConfigMapKey, clientConfigMap); err != nil {
		return "", fmt.Errorf("failed to get client ConfigMap %s: %w", clientConfigMapKey, err)
	}

	// Extract the YAML content
	yamlContent, exists := clientConfigMap.Data[services.FeatureStoreYamlCmKey]
	if !exists {
		return "", fmt.Errorf("client ConfigMap %s does not contain key %s", clientConfigMapName, services.FeatureStoreYamlCmKey)
	}

	return yamlContent, nil
}

// cleanupNotebookConfigMap removes the notebook-feast-config ConfigMap when integration label is completely removed and config annotation is empty
func (r *NotebookConfigMapReconciler) cleanupNotebookConfigMap(ctx context.Context, namespace, notebookName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	configMapName := fmt.Sprintf("%s%s", notebookName, NotebookConfigMapNameSuffix)
	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: namespace}, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := r.Delete(ctx, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to delete notebook ConfigMap", "ConfigMap", configMapName)
		return ctrl.Result{}, err
	}

	logger.Info("Deleted notebook ConfigMap (label removed)", "ConfigMap", configMapName, "namespace", namespace)
	return ctrl.Result{}, nil
}

// mapFeatureStoreToNotebookRequests maps a FeatureStore change to all Notebooks that reference its project
// This is called when a FeatureStore CR is created, updated, or deleted
func (r *NotebookConfigMapReconciler) mapFeatureStoreToNotebookRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)

	featureStore, ok := obj.(*feastdevv1.FeatureStore)
	if !ok {
		return nil
	}

	// Get the project name from the FeatureStore
	projectName := featureStore.Spec.FeastProject
	if featureStore.Status.Applied.FeastProject != "" {
		projectName = featureStore.Status.Applied.FeastProject
	}

	if projectName == "" {
		return nil
	}

	logger.V(1).Info("FeatureStore changed, finding affected Notebooks", "project", projectName, "featurestore", fmt.Sprintf("%s/%s", featureStore.Namespace, featureStore.Name))

	// List all Notebooks across all namespaces
	notebookList := &unstructured.UnstructuredList{}
	notebookList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   r.NotebookGVK.Group,
		Version: r.NotebookGVK.Version,
		Kind:    r.NotebookGVK.Kind + "List",
	})

	if err := r.List(ctx, notebookList, client.InNamespace("")); err != nil {
		logger.Error(err, "Failed to list Notebooks")
		return nil
	}

	var requests []reconcile.Request
	for i := range notebookList.Items {
		notebook := &notebookList.Items[i]
		labels := notebook.GetLabels()
		if labels == nil {
			continue
		}

		// Check if feast integration is enabled
		feastIntegrationEnabled, exists := labels[NotebookFeastIntegrationLabel]
		if !exists || feastIntegrationEnabled != TrueValue {
			continue
		}

		// Read projects from annotation
		annotations := notebook.GetAnnotations()
		if annotations == nil {
			continue
		}

		feastConfigAnnotation, exists := annotations[NotebookFeastConfigAnnotation]
		if !exists || feastConfigAnnotation == "" {
			continue
		}

		// Check if this notebook references the project
		projectNames := r.parseProjectNames(feastConfigAnnotation)
		for _, name := range projectNames {
			if name == projectName {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      notebook.GetName(),
						Namespace: notebook.GetNamespace(),
					},
				})
				break
			}
		}
	}

	if len(requests) > 0 {
		logger.Info("FeatureStore change triggers Notebook reconciliation", "project", projectName, "notebooks", len(requests))
	}

	return requests
}

// mapConfigMapToNotebookRequest maps a ConfigMap change back to its owning Notebook
// This ensures that if a user deletes or modifies the ConfigMap, it gets recreated/restored
func (r *NotebookConfigMapReconciler) mapConfigMapToNotebookRequest(ctx context.Context, obj client.Object) []reconcile.Request {
	configMap, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil
	}

	// Check if this is a notebook ConfigMap by checking the name suffix
	if !strings.HasSuffix(configMap.Name, NotebookConfigMapNameSuffix) {
		return nil
	}

	// Extract notebook name from ConfigMap name
	// ConfigMap name format: {notebook-name}-feast-config
	notebookName := strings.TrimSuffix(configMap.Name, NotebookConfigMapNameSuffix)
	if notebookName == "" {
		return nil
	}

	// Check if ConfigMap has owner reference to a Notebook
	// If it does, use that namespace, otherwise use ConfigMap's namespace
	namespace := configMap.Namespace
	for _, ownerRef := range configMap.OwnerReferences {
		if ownerRef.Kind == r.NotebookGVK.Kind {
			// Found Notebook owner reference
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      notebookName,
						Namespace: namespace,
					},
				},
			}
		}
	}

	// If no owner reference, still try to reconcile the notebook
	// (in case owner reference was removed or ConfigMap was recreated)
	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Name:      notebookName,
				Namespace: namespace,
			},
		},
	}
}

// SetupWithManager sets up the controller with the Manager
func (r *NotebookConfigMapReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Create a source for unstructured Notebook resources
	notebookSource := &unstructured.Unstructured{}
	notebookSource.SetGroupVersionKind(r.NotebookGVK)

	bldr := ctrl.NewControllerManagedBy(mgr).
		// For() watches Notebooks - any Notebook CRUD triggers Reconcile()
		// Reconcile() filters to only process Notebooks with opendatahub.io/feast-integration label set to "true"
		For(notebookSource).
		// Watch ConfigMaps - when a notebook ConfigMap is modified or deleted,
		// trigger reconciliation of the owning Notebook to restore it
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.mapConfigMapToNotebookRequest),
		).
		// Watch FeatureStores - when a FeatureStore changes (CRUD), find all Notebooks
		// that reference its project and trigger their reconciliation
		// This ensures notebook ConfigMaps are updated when FeatureStore client configs change
		Watches(
			&feastdevv1.FeatureStore{},
			handler.EnqueueRequestsFromMapFunc(r.mapFeatureStoreToNotebookRequests),
		)

	return bldr.Complete(r)
}

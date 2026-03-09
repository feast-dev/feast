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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	FeastNamespaceLabelKey   = "opendatahub.io/feast"
	FeastNamespaceLabelValue = "true"
)

// EnsureNamespaceLabel adds the Feast discovery label to the namespace.
// Call when a FeatureStore is successfully deployed.
func EnsureNamespaceLabel(ctx context.Context, c client.Client, namespace string) error {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}
	if ns.Labels == nil {
		ns.Labels = make(map[string]string)
	}
	if ns.Labels[FeastNamespaceLabelKey] == FeastNamespaceLabelValue {
		return nil
	}
	ns.Labels[FeastNamespaceLabelKey] = FeastNamespaceLabelValue
	if err := c.Update(ctx, ns); err != nil {
		return fmt.Errorf("failed to patch namespace %s with Feast label: %w", namespace, err)
	}
	log.FromContext(ctx).Info("Added Feast discovery label to namespace", "namespace", namespace)
	return nil
}

// RemoveNamespaceLabelIfLast removes the Feast label from the namespace when
// otherFeatureStoreCount is 0. Call when a FeatureStore is being deleted.
func RemoveNamespaceLabelIfLast(ctx context.Context, c client.Client, namespace string, otherFeatureStoreCount int) error {
	if otherFeatureStoreCount > 0 {
		return nil
	}
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}
	if ns.Labels == nil || ns.Labels[FeastNamespaceLabelKey] == "" {
		return nil
	}
	delete(ns.Labels, FeastNamespaceLabelKey)
	if err := c.Update(ctx, ns); err != nil {
		return fmt.Errorf("failed to remove Feast label from namespace %s: %w", namespace, err)
	}
	log.FromContext(ctx).Info("Removed Feast discovery label from namespace", "namespace", namespace)
	return nil
}

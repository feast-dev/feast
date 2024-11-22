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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (feast *FeastServices) deployClient() error {
	if err := feast.createClientConfigMap(); err != nil {
		return feast.setFeastServiceCondition(err, ClientFeastType)
	}
	return feast.setFeastServiceCondition(nil, ClientFeastType)
}

func (feast *FeastServices) createClientConfigMap() error {
	logger := log.FromContext(feast.Handler.Context)
	cm := &corev1.ConfigMap{
		ObjectMeta: feast.GetObjectMeta(ClientFeastType),
	}
	cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, cm, controllerutil.MutateFn(func() error {
		return feast.setClientConfigMap(cm)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ConfigMap", cm.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) setClientConfigMap(cm *corev1.ConfigMap) error {
	cm.Labels = feast.getLabels(ClientFeastType)
	clientYaml, err := feast.getClientFeatureStoreYaml()
	if err != nil {
		return err
	}
	cm.Data = map[string]string{FeatureStoreYamlCmKey: string(clientYaml)}
	feast.Handler.FeatureStore.Status.ClientConfigMap = cm.Name
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, cm, feast.Handler.Scheme)
}

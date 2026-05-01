/*
Copyright 2026 Feast Community.

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

// Package metrics provides a Prometheus info gauge that records the store
// types configured for each FeatureStore CR (online store, offline store,
// registry). These operator-level metrics are distinct from the Feast
// feature-server application metrics (feast_feature_server_*) and are useful
// for usage telemetry and assessing the impact of removing store type support.
package metrics

import (
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const typeNone = "none"

// FeatureStoreMetrics holds the Prometheus GaugeVec for feast-operator
// installation telemetry.
type FeatureStoreMetrics struct {
	FeatureStoreInfo *prometheus.GaugeVec
}

// NewFeatureStoreMetrics creates a new FeatureStoreMetrics with the GaugeVec
// initialised but not yet registered. Call Register() before starting the manager.
func NewFeatureStoreMetrics() *FeatureStoreMetrics {
	return &FeatureStoreMetrics{
		FeatureStoreInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "feast_operator_feature_store_info",
				Help: "Information about a deployed FeatureStore. " +
					"Value is always 1. Labels carry the configured store types: " +
					"'online_store_type', 'offline_store_type', and 'registry_type' " +
					"are set to the persistence type (e.g. redis, snowflake.offline, local) " +
					"or 'none' when that component is not configured.",
			},
			[]string{"namespace", "name", "online_store_type", "offline_store_type", "registry_type"},
		),
	}
}

// Register registers the metric with the controller-runtime metrics registry
// so it is exposed on the manager's /metrics endpoint.
func (m *FeatureStoreMetrics) Register() {
	ctrlmetrics.Registry.MustRegister(m.FeatureStoreInfo)
}

// RecordFeatureStore updates the gauge for the given FeatureStore using the
// applied configuration stored in status.Applied (which has operator defaults
// applied). The previous label set for this FeatureStore is deleted first so
// that store type changes are reflected cleanly on the next scrape.
func (m *FeatureStoreMetrics) RecordFeatureStore(fs *feastdevv1.FeatureStore) {
	svcs := fs.Status.Applied.Services
	m.FeatureStoreInfo.DeletePartialMatch(prometheus.Labels{
		"namespace": fs.Namespace,
		"name":      fs.Name,
	})
	m.FeatureStoreInfo.WithLabelValues(
		fs.Namespace,
		fs.Name,
		onlineStoreType(svcs),
		offlineStoreType(svcs),
		registryType(svcs),
	).Set(1)
}

// DeleteFeatureStore removes the metric label set for the given FeatureStore.
// Safe to call when the CR has already been deleted from the API server.
func (m *FeatureStoreMetrics) DeleteFeatureStore(namespace, name string) {
	m.FeatureStoreInfo.DeletePartialMatch(prometheus.Labels{
		"namespace": namespace,
		"name":      name,
	})
}

// onlineStoreType returns the online store persistence type or "none".
func onlineStoreType(svcs *feastdevv1.FeatureStoreServices) string {
	if svcs == nil || svcs.OnlineStore == nil {
		return typeNone
	}
	if p := svcs.OnlineStore.Persistence; p != nil && p.DBPersistence != nil {
		return p.DBPersistence.Type
	}
	return "file"
}

// offlineStoreType returns the offline store persistence type or "none".
func offlineStoreType(svcs *feastdevv1.FeatureStoreServices) string {
	if svcs == nil || svcs.OfflineStore == nil {
		return typeNone
	}
	if p := svcs.OfflineStore.Persistence; p != nil {
		if p.DBPersistence != nil {
			return p.DBPersistence.Type
		}
		if p.FilePersistence != nil && p.FilePersistence.Type != "" {
			return p.FilePersistence.Type
		}
	}
	return "file"
}

// registryType returns "local", "remote", "remote_feastref", or "none".
func registryType(svcs *feastdevv1.FeatureStoreServices) string {
	if svcs == nil || svcs.Registry == nil {
		return typeNone
	}
	switch {
	case svcs.Registry.Local != nil:
		return "local"
	case svcs.Registry.Remote != nil:
		if svcs.Registry.Remote.FeastRef != nil {
			return "remote_feastref"
		}
		return "remote"
	default:
		return typeNone
	}
}

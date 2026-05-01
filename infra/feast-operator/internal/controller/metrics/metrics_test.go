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

package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	. "github.com/feast-dev/feast/infra/feast-operator/internal/controller/metrics"
)

const testNamespace = "test-ns"

// gaugeValue reads the float64 value for the given label values.
// Returns -1 if the metric is not found.
func gaugeValue(gv *prometheus.GaugeVec, labels ...string) float64 {
	g, err := gv.GetMetricWithLabelValues(labels...)
	if err != nil {
		return -1
	}
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

func featureStore(name string, svcs *feastdevv1.FeatureStoreServices) *feastdevv1.FeatureStore {
	fs := &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name},
	}
	fs.Status.Applied.Services = svcs
	return fs
}

func TestRecordFeatureStore_NoServices(t *testing.T) {
	m := NewFeatureStoreMetrics()
	m.RecordFeatureStore(featureStore("fs", nil))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "none", "none"); v != 1 {
		t.Errorf("expected 1 for all-absent store, got %v", v)
	}
}

func TestRecordFeatureStore_OnlineStore_File(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "file", "none", "none"); v != 1 {
		t.Errorf("expected 1 for file online store, got %v", v)
	}
}

func TestRecordFeatureStore_OnlineStore_Redis(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "redis"},
			},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "redis", "none", "none"); v != 1 {
		t.Errorf("expected 1 for redis online store, got %v", v)
	}
}

func TestRecordFeatureStore_OfflineStore_DB(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				DBPersistence: &feastdevv1.OfflineStoreDBStorePersistence{Type: "snowflake.offline"},
			},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "snowflake.offline", "none"); v != 1 {
		t.Errorf("expected 1 for snowflake offline store, got %v", v)
	}
}

func TestRecordFeatureStore_OfflineStore_FilePersistenceType(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				FilePersistence: &feastdevv1.OfflineStoreFilePersistence{Type: "duckdb"},
			},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "duckdb", "none"); v != 1 {
		t.Errorf("expected 1 for duckdb offline store, got %v", v)
	}
}

func TestRecordFeatureStore_Registry_Local(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "none", "local"); v != 1 {
		t.Errorf("expected 1 for local registry, got %v", v)
	}
}

func TestRecordFeatureStore_Registry_RemoteHostname(t *testing.T) {
	hostname := "registry.example.com:443"
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Remote: &feastdevv1.RemoteRegistryConfig{Hostname: &hostname},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "none", "remote"); v != 1 {
		t.Errorf("expected 1 for remote registry, got %v", v)
	}
}

func TestRecordFeatureStore_Registry_RemoteFeastRef(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Remote: &feastdevv1.RemoteRegistryConfig{
				FeastRef: &feastdevv1.FeatureStoreRef{Name: "other-fs", Namespace: "other-ns"},
			},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "none", "none", "remote_feastref"); v != 1 {
		t.Errorf("expected 1 for remote_feastref registry, got %v", v)
	}
}

func TestRecordFeatureStore_AllComponents(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "redis"},
			},
		},
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				DBPersistence: &feastdevv1.OfflineStoreDBStorePersistence{Type: "snowflake.offline"},
			},
		},
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "redis", "snowflake.offline", "local"); v != 1 {
		t.Errorf("expected 1 for full store config, got %v", v)
	}
}

func TestRecordFeatureStore_TypeChange(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs1 := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "redis"},
			},
		},
	}
	svcs2 := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "postgres"},
			},
		},
	}

	m.RecordFeatureStore(featureStore("fs", svcs1))
	m.RecordFeatureStore(featureStore("fs", svcs2))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "redis", "none", "none"); v != 0 {
		t.Errorf("old label set (redis) should be removed after type change, got %v", v)
	}
	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "postgres", "none", "none"); v != 1 {
		t.Errorf("new label set (postgres) should be 1 after type change, got %v", v)
	}
}

func TestDeleteFeatureStore_RemovesMetric(t *testing.T) {
	m := NewFeatureStoreMetrics()
	svcs := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "redis"},
			},
		},
	}
	m.RecordFeatureStore(featureStore("fs", svcs))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "redis", "none", "none"); v != 1 {
		t.Fatalf("setup: expected 1 before delete, got %v", v)
	}

	m.DeleteFeatureStore(testNamespace, "fs")

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs", "redis", "none", "none"); v != 0 {
		t.Errorf("expected 0 after DeleteFeatureStore, got %v", v)
	}
}

func TestMultipleFeatureStores_IndependentLabelSets(t *testing.T) {
	m := NewFeatureStoreMetrics()

	svcs1 := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "redis"},
			},
		},
	}
	svcs2 := &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{Type: "postgres"},
			},
		},
	}

	m.RecordFeatureStore(featureStore("fs-1", svcs1))
	m.RecordFeatureStore(featureStore("fs-2", svcs2))

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs-1", "redis", "none", "none"); v != 1 {
		t.Errorf("fs-1: expected redis=1, got %v", v)
	}
	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs-2", "postgres", "none", "none"); v != 1 {
		t.Errorf("fs-2: expected postgres=1, got %v", v)
	}

	m.DeleteFeatureStore(testNamespace, "fs-1")

	if v := gaugeValue(m.FeatureStoreInfo, testNamespace, "fs-2", "postgres", "none", "none"); v != 1 {
		t.Errorf("fs-2 should be unaffected after fs-1 deletion, got %v", v)
	}
}

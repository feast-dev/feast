package metrics

import (
	"log"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

const (
	LookupNotFoundMetric      = "mlpfs.featureserver.feature_lookup_not_found"
	LookupNullOrExpiredMetric = "mlpfs.featureserver.feature_lookup_null_or_expired"
	LookupRequestsMetric      = "mlpfs.featureserver.feature_lookup_requests"
)

// LookupMetricsAggregator accumulates per-feature lookup statuses for a single request and emits aggregated counts.
type LookupMetricsAggregator struct {
	notFound         map[string]int64
	nullOrExpired    map[string]int64
	total            map[string]int64
	featureToViewMap map[string]string
	baseTags         []string
	client           StatsdClient
	sampleRate       float64
}

func NewLookupMetricsAggregator(
	project, onlineStore string,
	client StatsdClient,
	sampleRate float64,
) *LookupMetricsAggregator {
	if client == nil {
		return nil
	}

	return &LookupMetricsAggregator{
		notFound:         make(map[string]int64),
		nullOrExpired:    make(map[string]int64),
		total:            make(map[string]int64),
		featureToViewMap: make(map[string]string),
		baseTags: []string{
			"project:" + project,
			"online_store_type:" + onlineStore,
		},
		client:     client,
		sampleRate: sampleRate,
	}
}

func (m *LookupMetricsAggregator) Record(featureID, featureViewName string, status serving.FieldStatus) {
	if m == nil {
		return
	}
	m.total[featureID]++
	// Store FV name mapping if not present
	if _, exists := m.featureToViewMap[featureID]; !exists {
		m.featureToViewMap[featureID] = featureViewName
	}
	switch status {
	case serving.FieldStatus_NOT_FOUND:
		m.notFound[featureID]++
	case serving.FieldStatus_NULL_VALUE, serving.FieldStatus_OUTSIDE_MAX_AGE:
		m.nullOrExpired[featureID]++
	}
}

func (m *LookupMetricsAggregator) RecordFromFeatureVectors(vectors []*onlineserving.FeatureVector) {
	if m == nil {
		return
	}
	for _, vector := range vectors {
		for _, status := range vector.Statuses {
			m.Record(vector.Name, vector.FeatureViewName, status)
		}
	}
}

func (m *LookupMetricsAggregator) RecordFromRangeFeatureVectors(vectors []*onlineserving.RangeFeatureVector) {
	if m == nil {
		return
	}
	for _, vector := range vectors {
		for _, entityStatuses := range vector.RangeStatuses {
			for _, status := range entityStatuses {
				m.Record(vector.Name, vector.FeatureViewName, status)
			}
		}
	}
}

func (m *LookupMetricsAggregator) featureTags(featureID, featureViewName string) []string {
	tags := make([]string, len(m.baseTags)+2)
	copy(tags, m.baseTags)
	tags[len(m.baseTags)] = "feature:" + featureID
	if featureViewName == "" {
		featureViewName = "unknown"
		log.Printf("WARNING: Lookup metrics feature_view tag set to 'unknown' for feature: %s. This may indicate FeatureViewName was not populated.", featureID)
	}
	tags[len(m.baseTags)+1] = "feature_view:" + featureViewName
	return tags
}

func (m *LookupMetricsAggregator) Emit() {
	if m == nil || m.client == nil {
		return
	}

	for featureID, count := range m.notFound {
		if count > 0 {
			fvName := m.featureToViewMap[featureID]
			m.client.Count(LookupNotFoundMetric, count, m.featureTags(featureID, fvName), m.sampleRate)
		}
	}
	for featureID, count := range m.nullOrExpired {
		if count > 0 {
			fvName := m.featureToViewMap[featureID]
			m.client.Count(LookupNullOrExpiredMetric, count, m.featureTags(featureID, fvName), m.sampleRate)
		}
	}
	for featureID, count := range m.total {
		if count > 0 {
			fvName := m.featureToViewMap[featureID]
			m.client.Count(LookupRequestsMetric, count, m.featureTags(featureID, fvName), m.sampleRate)
		}
	}
}

package metrics

import (
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

const (
	LookupNotFoundMetric      = "mlpfs.featureserver.feature_lookup_not_found"
	LookupNullOrExpiredMetric = "mlpfs.featureserver.feature_lookup_null_or_expired"
	LookupRequestsMetric      = "mlpfs.featureserver.feature_lookup_requests"
)

// extractFeatureView extracts the feature view name from a full feature name.
// Feature names follow the format: feature_view__feature_name
// Example: "hotel_fv__price" -> "hotel_fv"
func extractFeatureView(featureName string) string {
	parts := strings.SplitN(featureName, "__", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return "unknown"
}

// LookupMetricsAggregator accumulates per-feature lookup statuses for a single request and emits aggregated counts.
type LookupMetricsAggregator struct {
	notFound      map[string]int64
	nullOrExpired map[string]int64
	total         map[string]int64
	baseTags      []string
	client        StatsdClient
	sampleRate    float64
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
		notFound:      make(map[string]int64),
		nullOrExpired: make(map[string]int64),
		total:         make(map[string]int64),
		baseTags: []string{
			"project:" + project,
			"online_store_type:" + onlineStore,
		},
		client:     client,
		sampleRate: sampleRate,
	}
}

func (m *LookupMetricsAggregator) Record(featureID string, status serving.FieldStatus) {
	if m == nil {
		return
	}
	m.total[featureID]++
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
			m.Record(vector.Name, status)
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
				m.Record(vector.Name, status)
			}
		}
	}
}

func (m *LookupMetricsAggregator) featureTags(featureID string) []string {
	tags := make([]string, len(m.baseTags)+2)
	copy(tags, m.baseTags)
	tags[len(m.baseTags)] = "feature:" + featureID
	tags[len(m.baseTags)+1] = "feature_view:" + extractFeatureView(featureID)
	return tags
}

func (m *LookupMetricsAggregator) Emit() {
	if m == nil || m.client == nil {
		return
	}

	for featureID, count := range m.notFound {
		if count > 0 {
			m.client.Count(LookupNotFoundMetric, count, m.featureTags(featureID), m.sampleRate)
		}
	}
	for featureID, count := range m.nullOrExpired {
		if count > 0 {
			m.client.Count(LookupNullOrExpiredMetric, count, m.featureTags(featureID), m.sampleRate)
		}
	}
	for featureID, count := range m.total {
		if count > 0 {
			m.client.Count(LookupRequestsMetric, count, m.featureTags(featureID), m.sampleRate)
		}
	}
}

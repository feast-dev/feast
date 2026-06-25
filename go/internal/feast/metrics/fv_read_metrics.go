package metrics

const (
	FVReadLatencyMetric  = "mlpfs.featureserver.fv_read_latency_ms"
	FVReadRequestsMetric = "mlpfs.featureserver.fv_read_requests"
	FVReadErrorsMetric   = "mlpfs.featureserver.fv_read_errors"
)

// FeatureViewReadMetrics emits per-feature-view read metrics (latency, requests, errors).
type FeatureViewReadMetrics struct {
	baseTags   []string
	client     StatsdClient
	sampleRate float64
}

func NewFeatureViewReadMetrics(project, onlineStore string, client StatsdClient, sampleRate float64) *FeatureViewReadMetrics {
	if client == nil {
		return nil
	}

	return &FeatureViewReadMetrics{
		baseTags: []string{
			"project:" + project,
			"online_store_type:" + onlineStore,
		},
		client:     client,
		sampleRate: sampleRate,
	}
}

func (m *FeatureViewReadMetrics) fvTags(fvName string) []string {
	tags := make([]string, len(m.baseTags)+1)
	copy(tags, m.baseTags)
	tags[len(m.baseTags)] = "feature_view:" + fvName
	return tags
}

func (m *FeatureViewReadMetrics) Emit(featureViewNames []string, latencyMs float64, hasError bool) {
	if m == nil || m.client == nil {
		return
	}

	for _, fvName := range featureViewNames {
		tags := m.fvTags(fvName)
		m.client.Distribution(FVReadLatencyMetric, latencyMs, tags, m.sampleRate)
		m.client.Count(FVReadRequestsMetric, 1, tags, m.sampleRate)
		if hasError {
			m.client.Count(FVReadErrorsMetric, 1, tags, m.sampleRate)
		}
	}
}

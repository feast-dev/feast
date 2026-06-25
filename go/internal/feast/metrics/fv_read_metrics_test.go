package metrics

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFVMetrics_EmitLatencyDistribution(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake, 1.0)

	m.Emit([]string{"hotel_fv"}, 42.5, false)

	assert.Len(t, fake.distCalls, 1)
	assert.Equal(t, FVReadLatencyMetric, fake.distCalls[0].name)
	assert.Equal(t, 42.5, fake.distCalls[0].value)
	assert.Contains(t, fake.distCalls[0].tags, "project:proj")
	assert.Contains(t, fake.distCalls[0].tags, "online_store_type:redis")
	assert.Contains(t, fake.distCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_EmitRequestsCount(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake, 1.0)

	m.Emit([]string{"hotel_fv"}, 10.0, false)

	requestCalls := filterCalls(fake.calls, FVReadRequestsMetric)
	assert.Len(t, requestCalls, 1)
	assert.Equal(t, int64(1), requestCalls[0].value)
	assert.Contains(t, requestCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_EmitErrors(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake, 1.0)

	m.Emit([]string{"hotel_fv"}, 100.0, true)

	errorCalls := filterCalls(fake.calls, FVReadErrorsMetric)
	assert.Len(t, errorCalls, 1)
	assert.Equal(t, int64(1), errorCalls[0].value)
	assert.Contains(t, errorCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_NoErrorsWhenSuccess(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake, 1.0)

	m.Emit([]string{"hotel_fv"}, 50.0, false)

	errorCalls := filterCalls(fake.calls, FVReadErrorsMetric)
	assert.Len(t, errorCalls, 0)
}

func TestFVMetrics_MultipleFeatureViews(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "valkey", fake, 1.0)

	m.Emit([]string{"hotel_fv", "user_fv", "booking_fv"}, 75.0, false)

	// 3 latency distributions
	assert.Len(t, fake.distCalls, 3)

	// 3 request counts
	requestCalls := filterCalls(fake.calls, FVReadRequestsMetric)
	assert.Len(t, requestCalls, 3)

	// All have same latency
	for _, dc := range fake.distCalls {
		assert.Equal(t, 75.0, dc.value)
	}

	// Check each FV is tagged
	fvTags := make(map[string]bool)
	for _, dc := range fake.distCalls {
		fvTags[findTag(dc.tags, "feature_view:")] = true
	}
	assert.True(t, fvTags["hotel_fv"])
	assert.True(t, fvTags["user_fv"])
	assert.True(t, fvTags["booking_fv"])
}

func TestFVMetrics_NilClient(t *testing.T) {
	m := NewFeatureViewReadMetrics("proj", "redis", nil, 1.0)
	assert.Nil(t, m)
}

func TestFVMetrics_NilSafe(t *testing.T) {
	var m *FeatureViewReadMetrics
	m.Emit([]string{"fv"}, 10.0, false) // should not panic
}

func TestFVMetrics_Sampling(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake, 0.5)

	assert.Equal(t, 0.5, m.sampleRate)

	m.Emit([]string{"fv"}, 10.0, false)

	// Always emits; sample rate is passed to the statsd client, not used as a local guard.
	assert.Len(t, fake.distCalls, 1)
	assert.Equal(t, float64(0.5), fake.distCalls[0].rate)
	assert.Len(t, fake.calls, 1)
	assert.Equal(t, float64(0.5), fake.calls[0].rate)
}

func TestIsFVMetricsEnabled(t *testing.T) {
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	assert.False(t, IsFVMetricsEnabled())

	os.Setenv("ENABLE_FV_LEVEL_METRICS", "true")
	assert.True(t, IsFVMetricsEnabled())
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")

	// ENABLE_MISSING_KEY_METRICS no longer implies IsFVMetricsEnabled.
	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	assert.False(t, IsFVMetricsEnabled())
	assert.True(t, IsMetricsClientEnabled())
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
}

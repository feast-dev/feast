package metrics

import (
	"os"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
)

type metricCall struct {
	name  string
	value int64
	tags  []string
	rate  float64
}

type distCall struct {
	name  string
	value float64
	tags  []string
	rate  float64
}

type fakeStatsdClient struct {
	calls     []metricCall
	distCalls []distCall
}

func (f *fakeStatsdClient) Count(name string, value int64, tags []string, rate float64) error {
	f.calls = append(f.calls, metricCall{name: name, value: value, tags: tags, rate: rate})
	return nil
}

func (f *fakeStatsdClient) Distribution(name string, value float64, tags []string, rate float64) error {
	f.distCalls = append(f.distCalls, distCall{name: name, value: value, tags: tags, rate: rate})
	return nil
}

func newTestAggregator(client StatsdClient) *LookupMetricsAggregator {
	return NewLookupMetricsAggregator("test_project", "redis", client, 1.0)
}

func TestAggregator_AllNotFound(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	assert.Equal(t, int64(3), notFoundCalls[0].value)
	assert.Contains(t, notFoundCalls[0].tags, "feature:user_fv__age")

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 1)
	assert.Equal(t, int64(3), totalCalls[0].value)
	assert.Contains(t, totalCalls[0].tags, "feature:user_fv__age")
	assert.Contains(t, totalCalls[0].tags, "feature_view:user_fv")
}

func TestAggregator_AllNullOrExpired(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("order_fv__amt", serving.FieldStatus_NULL_VALUE)
	agg.Record("order_fv__amt", serving.FieldStatus_NULL_VALUE)
	agg.Record("order_fv__amt", serving.FieldStatus_OUTSIDE_MAX_AGE)
	agg.Emit()

	nullCalls := filterCalls(fake.calls, LookupNullOrExpiredMetric)
	assert.Len(t, nullCalls, 1)
	assert.Equal(t, int64(3), nullCalls[0].value)
	assert.Contains(t, nullCalls[0].tags, "feature:order_fv__amt")

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 1)
	assert.Equal(t, int64(3), totalCalls[0].value)
	assert.Contains(t, totalCalls[0].tags, "feature:order_fv__amt")
	assert.Contains(t, totalCalls[0].tags, "feature_view:order_fv")
}

func TestAggregator_MixedStatuses(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("fv_a__f1", serving.FieldStatus_PRESENT)
	agg.Record("fv_a__f1", serving.FieldStatus_NOT_FOUND)
	agg.Record("fv_b__f2", serving.FieldStatus_NULL_VALUE)
	agg.Record("fv_b__f2", serving.FieldStatus_PRESENT)
	agg.Record("fv_b__f2", serving.FieldStatus_OUTSIDE_MAX_AGE)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	assert.Equal(t, int64(1), notFoundCalls[0].value)

	nullCalls := filterCalls(fake.calls, LookupNullOrExpiredMetric)
	assert.Len(t, nullCalls, 1)
	assert.Equal(t, int64(2), nullCalls[0].value)

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 2)
	totalByFeature := map[string]int64{}
	for _, c := range totalCalls {
		totalByFeature[findTag(c.tags, "feature:")] = c.value
	}
	assert.Equal(t, int64(2), totalByFeature["fv_a__f1"])
	assert.Equal(t, int64(3), totalByFeature["fv_b__f2"])
}

func TestAggregator_AllPresent(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("fv__f1", serving.FieldStatus_PRESENT)
	agg.Record("fv__f1", serving.FieldStatus_PRESENT)
	agg.Emit()

	// No not_found or null_or_expired calls
	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	nullCalls := filterCalls(fake.calls, LookupNullOrExpiredMetric)
	assert.Len(t, notFoundCalls, 0)
	assert.Len(t, nullCalls, 0)

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 1)
	assert.Equal(t, int64(2), totalCalls[0].value)
	assert.Contains(t, totalCalls[0].tags, "feature:fv__f1")
	assert.Contains(t, totalCalls[0].tags, "feature_view:fv")
}

func TestAggregator_NilSafe(t *testing.T) {
	var agg *LookupMetricsAggregator
	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.RecordFromFeatureVectors(nil)
	agg.RecordFromRangeFeatureVectors(nil)
	agg.Emit()
}

func TestAggregator_NilClient(t *testing.T) {
	agg := NewLookupMetricsAggregator("p", "r", nil, 1.0)
	assert.Nil(t, agg)
}

func TestAggregator_Tags(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := NewLookupMetricsAggregator("mlpfs", "eg-valkey", fake, 1.0)

	agg.Record("hotel_fv__price", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	tags := notFoundCalls[0].tags
	assert.Contains(t, tags, "project:mlpfs")
	assert.Contains(t, tags, "online_store_type:eg-valkey")
	assert.Contains(t, tags, "feature:hotel_fv__price")
	assert.Contains(t, tags, "feature_view:hotel_fv")
}

func TestRecordFromFeatureVectors(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	vectors := []*onlineserving.FeatureVector{
		{
			Name:     "fv_a__f1",
			Statuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_NOT_FOUND},
		},
		{
			Name:     "fv_a__f2",
			Statuses: []serving.FieldStatus{serving.FieldStatus_NOT_FOUND, serving.FieldStatus_NOT_FOUND},
		},
	}

	agg.RecordFromFeatureVectors(vectors)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 2)

	callsByFeature := map[string]int64{}
	for _, c := range notFoundCalls {
		callsByFeature[findTag(c.tags, "feature:")] = c.value
	}
	assert.Equal(t, int64(1), callsByFeature["fv_a__f1"])
	assert.Equal(t, int64(2), callsByFeature["fv_a__f2"])

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 2)
	totalByFeature := map[string]int64{}
	for _, c := range totalCalls {
		totalByFeature[findTag(c.tags, "feature:")] = c.value
	}
	assert.Equal(t, int64(2), totalByFeature["fv_a__f1"])
	assert.Equal(t, int64(2), totalByFeature["fv_a__f2"])
}

func TestRecordFromRangeFeatureVectors(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	vectors := []*onlineserving.RangeFeatureVector{
		{
			Name: "sfv__f1",
			RangeStatuses: [][]serving.FieldStatus{
				{serving.FieldStatus_PRESENT, serving.FieldStatus_NOT_FOUND},
				{serving.FieldStatus_NOT_FOUND},
			},
		},
	}

	agg.RecordFromRangeFeatureVectors(vectors)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	assert.Equal(t, int64(2), notFoundCalls[0].value)

	totalCalls := filterCalls(fake.calls, LookupRequestsMetric)
	assert.Len(t, totalCalls, 1)
	assert.Equal(t, int64(3), totalCalls[0].value)
	assert.Contains(t, totalCalls[0].tags, "feature:sfv__f1")
	assert.Contains(t, totalCalls[0].tags, "feature_view:sfv")
}

func TestIsMissingKeyMetricsEnabled(t *testing.T) {
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	assert.False(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	assert.True(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "TRUE")
	assert.True(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "false")
	assert.False(t, IsMissingKeyMetricsEnabled())

	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
}

func TestGetOnlineStoreType(t *testing.T) {
}

func TestExtractFeatureView(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard format", "hotel_fv__price", "hotel_fv"},
		{"with underscore in feature", "hotel_fv__review_score_avg", "hotel_fv"},
		{"multiple feature views", "user_fv__age", "user_fv"},
		{"long feature view name", "ranking_signals_fv__score", "ranking_signals_fv"},
		{"no double underscore", "age", "unknown"},
		{"colon separator", "hotel_fv:price", "unknown"},
		{"empty string", "", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFeatureView(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// findTag extracts the value portion of a tag matching the given prefix.
func findTag(tags []string, prefix string) string {
	for _, tag := range tags {
		if len(tag) > len(prefix) && tag[:len(prefix)] == prefix {
			return tag[len(prefix):]
		}
	}
	return ""
}

// filterCalls returns only metric calls matching the given metric name.
func filterCalls(calls []metricCall, name string) []metricCall {
	var result []metricCall
	for _, c := range calls {
		if c.name == name {
			result = append(result, c)
		}
	}
	return result
}

func TestParseLookupSampleRate_Default(t *testing.T) {
	os.Unsetenv("FEAST_METRICS_SAMPLE_RATE")
	assert.Equal(t, DefaultLookupSampleRate, ParseLookupSampleRate(), "Default should be 0.01")
}

func TestParseLookupSampleRate_ReadFromEnv(t *testing.T) {
	os.Setenv("FEAST_METRICS_SAMPLE_RATE", "0.5")
	defer os.Unsetenv("FEAST_METRICS_SAMPLE_RATE")

	assert.Equal(t, 0.5, ParseLookupSampleRate())
}

func TestParseLookupSampleRate_InvalidValues(t *testing.T) {
	testCases := []struct {
		value    string
		expected float64
	}{
		{"-0.5", DefaultLookupSampleRate},
		{"1.5", DefaultLookupSampleRate},
		{"0", DefaultLookupSampleRate},
		{"abc", DefaultLookupSampleRate},
		{"", DefaultLookupSampleRate},
	}

	for _, tc := range testCases {
		t.Run(tc.value, func(t *testing.T) {
			if tc.value == "" {
				os.Unsetenv("FEAST_METRICS_SAMPLE_RATE")
			} else {
				os.Setenv("FEAST_METRICS_SAMPLE_RATE", tc.value)
				defer os.Unsetenv("FEAST_METRICS_SAMPLE_RATE")
			}

			assert.Equal(t, tc.expected, ParseLookupSampleRate())
		})
	}
}

func TestSampling_AdjustsCountsCorrectly(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := NewLookupMetricsAggregator("test_project", "redis", fake, 0.5)

	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	// Raw count is passed; statsd agent scales using the sample rate parameter.
	assert.Equal(t, int64(2), notFoundCalls[0].value)
	assert.Equal(t, float64(0.5), notFoundCalls[0].rate)
}

func TestSampling_NoAdjustmentWhenNotSampling(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := NewLookupMetricsAggregator("test_project", "redis", fake, 1.0)

	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	notFoundCalls := filterCalls(fake.calls, LookupNotFoundMetric)
	assert.Len(t, notFoundCalls, 1)
	assert.Equal(t, int64(2), notFoundCalls[0].value, "Count should not be adjusted with sample_rate=1.0")
}

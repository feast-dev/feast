//go:build !integration

package server

import (
	"os"
	"sort"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/metrics"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/stretchr/testify/assert"
)

type fakeStatsdClient struct{}

func (f *fakeStatsdClient) Count(string, int64, []string, float64) error          { return nil }
func (f *fakeStatsdClient) Distribution(string, float64, []string, float64) error { return nil }

func TestNewLookupAggregator_NilWhenMissingKeyMetricsDisabled(t *testing.T) {
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	mc := &MetricsContext{
		MissingKeyMetricsEnabled: metrics.IsMissingKeyMetricsEnabled(),
		Project:                  "test",
		OnlineStore:              "redis",
		Client:                   &fakeStatsdClient{},
		SampleRate:               1.0,
	}
	assert.Nil(t, mc.NewLookupAggregator())
}

func TestNewLookupAggregator_NonNilWhenMissingKeyMetricsEnabled(t *testing.T) {
	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	defer os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	mc := &MetricsContext{
		MissingKeyMetricsEnabled: metrics.IsMissingKeyMetricsEnabled(),
		Project:                  "test",
		OnlineStore:              "redis",
		Client:                   &fakeStatsdClient{},
		SampleRate:               1.0,
	}
	assert.NotNil(t, mc.NewLookupAggregator())
}

func TestNewMetricsContext_FVReadMetricsNilWhenFVMetricsDisabled(t *testing.T) {
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	defer os.Unsetenv("ENABLE_MISSING_KEY_METRICS")

	config := &registry.RepoConfig{Project: "test"}
	mc := NewMetricsContext(&fakeStatsdClient{}, config)

	assert.NotNil(t, mc)
	assert.Nil(t, mc.FVReadMetrics)
	assert.True(t, mc.MissingKeyMetricsEnabled)
}

func TestNewMetricsContext_LookupMetricsDisabledWhenMissingKeyMetricsDisabled(t *testing.T) {
	os.Setenv("ENABLE_FV_LEVEL_METRICS", "true")
	defer os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")

	config := &registry.RepoConfig{Project: "test"}
	mc := NewMetricsContext(&fakeStatsdClient{}, config)

	assert.NotNil(t, mc)
	assert.NotNil(t, mc.FVReadMetrics)
	assert.False(t, mc.MissingKeyMetricsEnabled)
	assert.Nil(t, mc.NewLookupAggregator())
}

func sortedStrings(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}

func TestExtractFVNamesFromRequest_FeatureRefs(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating", "user_fv:age"}, nil)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_NoColon(t *testing.T) {
	// refs without ":" are ignored (not a valid feature ref)
	names := extractFVNamesFromRequest([]string{"hotel_fv_price"}, nil)
	assert.Empty(t, names)
}

func TestExtractFVNamesFromRequest_FeatureService(t *testing.T) {
	fs := &model.FeatureService{
		Projections: []*model.FeatureViewProjection{
			{Name: "hotel_fv", NameAlias: ""},
			{Name: "user_fv", NameAlias: ""},
		},
	}
	names := extractFVNamesFromRequest(nil, fs)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_Deduplication(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating"}, nil)
	assert.Equal(t, []string{"hotel_fv"}, names)
}

func TestExtractFVNamesFromRequest_Empty(t *testing.T) {
	names := extractFVNamesFromRequest(nil, nil)
	assert.Empty(t, names)
}

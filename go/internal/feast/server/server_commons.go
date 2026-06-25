package server

import (
	"net/http"
	"os"
	"strings"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/feast-dev/feast/go/internal/feast/metrics"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func LogWithSpanContext(span *tracer.Span) zerolog.Logger {
	spanContext := span.Context()

	var logger = zerolog.New(os.Stderr).With().
		Int64("trace_id", int64(spanContext.TraceIDLower())).
		Int64("span_id", int64(spanContext.SpanID())).
		Timestamp().
		Logger()

	return logger
}

// MetricsContext holds pre-initialized metrics objects for the server lifetime.
type MetricsContext struct {
	FVReadMetrics            *metrics.FeatureViewReadMetrics
	SampleRate               float64
	Project                  string
	OnlineStore              string
	Client                   metrics.StatsdClient
	MissingKeyMetricsEnabled bool
}

func NewMetricsContext(client metrics.StatsdClient, config *registry.RepoConfig) *MetricsContext {
	if client == nil || config == nil {
		return nil
	}
	project := config.Project
	onlineStore := metrics.GetOnlineStoreType(config)

	var fvReadMetrics *metrics.FeatureViewReadMetrics
	if metrics.IsFVMetricsEnabled() {
		fvReadMetrics = metrics.NewFeatureViewReadMetrics(project, onlineStore, client, metrics.ParseFVSampleRate())
	}

	return &MetricsContext{
		FVReadMetrics:            fvReadMetrics,
		SampleRate:               metrics.ParseLookupSampleRate(),
		Project:                  project,
		OnlineStore:              onlineStore,
		Client:                   client,
		MissingKeyMetricsEnabled: metrics.IsMissingKeyMetricsEnabled(),
	}
}

func (mc *MetricsContext) NewLookupAggregator() *metrics.LookupMetricsAggregator {
	if mc == nil || !mc.MissingKeyMetricsEnabled {
		return nil
	}
	return metrics.NewLookupMetricsAggregator(mc.Project, mc.OnlineStore, mc.Client, mc.SampleRate)
}

func (mc *MetricsContext) EmitFVReadMetrics(fvNames []string, latencyMs float64, hasError bool) {
	if mc == nil {
		return
	}
	mc.FVReadMetrics.Emit(fvNames, latencyMs, hasError)
}

func (mc *MetricsContext) EmitLookupMetrics(vectors []*onlineserving.FeatureVector) {
	if mc == nil {
		return
	}
	agg := mc.NewLookupAggregator()
	agg.RecordFromFeatureVectors(vectors)
	agg.Emit()
}

func (mc *MetricsContext) EmitRangeLookupMetrics(vectors []*onlineserving.RangeFeatureVector) {
	if mc == nil {
		return
	}
	agg := mc.NewLookupAggregator()
	agg.RecordFromRangeFeatureVectors(vectors)
	agg.Emit()
}

func extractFVNamesFromRequest(features []string, featureService *model.FeatureService) []string {
	seen := make(map[string]struct{})

	for _, ref := range features {
		if parts := strings.SplitN(ref, ":", 2); len(parts) == 2 {
			seen[parts[0]] = struct{}{}
		}
	}

	if featureService != nil {
		for _, proj := range featureService.Projections {
			seen[proj.NameToUse()] = struct{}{}
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	return names
}

func CommonHttpHandlers(s *HttpServer, healthCheckHandler http.HandlerFunc) []Handler {
	return []Handler{
		{
			Path:        "/get-online-features",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeatures)),
		},
		{
			Path:        "/get-online-features-range",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeaturesRange)),
		},
		{
			Path:        "/version",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getVersion)),
		},
		{
			Path:        "/metrics",
			HandlerFunc: promhttp.Handler(),
		},
		{
			Path:        "/health",
			HandlerFunc: healthCheckHandler,
		},
	}
}

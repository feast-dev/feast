package metrics

import (
	"reflect"
	"time"

	"github.com/cabify/gotoprom"
	"github.com/cabify/gotoprom/prometheusvanilla"
	"github.com/prometheus/client_golang/prometheus"
)

var Metrics struct {
	HttpDuration func(HttpLabels) TimeHistogram `name:"feast_http_request_duration_seconds" help:"Time taken to serve HTTP requests" buckets:".005,.01,.025,.05,.1,.25,.5,1,2.5,5,10"`

	GrpcDuration func(GrpcLabels) TimeHistogram `name:"feast_grpc_request_duration_seconds" help:"Time taken to serve gRPC requests" buckets:".005,.01,.025,.05,.1,.25,.5,1,2.5,5,10"`

	HttpRequestsTotal func(HttpLabels) prometheus.Counter `name:"feast_http_requests_total" help:"Total number of HTTP requests"`

	GrpcRequestsTotal func(GrpcLabels) prometheus.Counter `name:"feast_grpc_requests_total" help:"Total number of gRPC requests"`
}

type HttpLabels struct {
	Method string `label:"method"`
	Status int    `label:"status"`
	Path   string `label:"path"`
}

type GrpcLabels struct {
	Service string `label:"service"`
	Method  string `label:"method"`
	Code    string `label:"code"`
}

func InitMetrics() {
	gotoprom.MustInit(&Metrics, "feast")
}

// TimeHistogram boilerplate from gotoprom README

var (
	// TimeHistogramType is the reflect.Type of the TimeHistogram interface
	TimeHistogramType = reflect.TypeOf((*TimeHistogram)(nil)).Elem()
)

func init() {
	gotoprom.MustAddBuilder(TimeHistogramType, RegisterTimeHistogram)
}

// RegisterTimeHistogram registers a TimeHistogram after registering the underlying prometheus.Histogram in the prometheus.Registerer provided
// The function it returns returns a TimeHistogram type as an interface{}
func RegisterTimeHistogram(name, help, namespace string, labelNames []string, tag reflect.StructTag) (func(prometheus.Labels) interface{}, prometheus.Collector, error) {
	f, collector, err := prometheusvanilla.BuildHistogram(name, help, namespace, labelNames, tag)
	if err != nil {
		return nil, nil, err
	}

	return func(labels prometheus.Labels) interface{} {
		return timeHistogramAdapter{Histogram: f(labels).(prometheus.Histogram)}
	}, collector, nil
}

// TimeHistogram offers the basic prometheus.Histogram functionality
// with additional time-observing functions
type TimeHistogram interface {
	prometheus.Histogram
	// Duration observes the duration in seconds
	Duration(duration time.Duration)
	// Since observes the duration in seconds since the time point provided
	Since(time.Time)
}

type timeHistogramAdapter struct {
	prometheus.Histogram
}

// Duration observes the duration in seconds
func (to timeHistogramAdapter) Duration(duration time.Duration) {
	to.Observe(duration.Seconds())
}

// Since observes the duration in seconds since the time point provided
func (to timeHistogramAdapter) Since(duration time.Time) {
	to.Duration(time.Since(duration))
}

package server

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func LogWithSpanContext(span tracer.Span) zerolog.Logger {
	spanContext := span.Context()

	var logger = zerolog.New(os.Stderr).With().
		Int64("trace_id", int64(spanContext.TraceID())).
		Int64("span_id", int64(spanContext.SpanID())).
		Timestamp().
		Logger()

	return logger
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

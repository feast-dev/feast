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

func CommonHttpHandlers(s *httpServer, healthCheckHandler http.HandlerFunc) []Handler {
	return []Handler{
		{
			path:        "/get-online-features",
			handlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeatures)),
		},
		{
			path:        "/get-online-features-range",
			handlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeaturesRange)),
		},
		{
			path:        "/metrics",
			handlerFunc: promhttp.Handler(),
		},
		{
			path:        "/health",
			handlerFunc: healthCheckHandler,
		},
	}
}

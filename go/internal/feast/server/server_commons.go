package server

import (
	"os"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/feast-dev/feast/go/server")

func LogWithSpanContext(span trace.Span) zerolog.Logger {
	spanContext := span.SpanContext()

	var logger = zerolog.New(os.Stderr).With().
		Str("trace_id", spanContext.TraceID().String()).
		Str("span_id", spanContext.SpanID().String()).
		Timestamp().
		Logger()

	return logger
}

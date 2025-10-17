package server

import (
	"os"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("github.com/feast-dev/feast/go/server")

func LogWithSpanContext(span trace.Span) zerolog.Logger {
	spanContext := span.SpanContext()

	var logger = zerolog.New(os.Stderr).With().
		Timestamp().
		Logger().
		Hex("trace_id", spanContext.TraceID()).
		Hex("span_id", spanContext.SpanID()).
		Timestamp().
		Logger()

	return logger
}

package server

import (
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

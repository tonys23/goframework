package goframework

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type GfLogger struct {
	*zap.Logger
}

func WrapWithContext(logger *zap.Logger) *GfLogger {
	return &GfLogger{
		logger,
	}

}

func (log *GfLogger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	ctx = getContext(ctx)
	span := trace.SpanFromContext(ctx)
	allFields := []zap.Field{}
	allFields = append(allFields, fields...)
	if span.IsRecording() {
		context := span.SpanContext()
		spanField := zap.String("spanid", context.SpanID().String())
		traceField := zap.String("traceid", context.TraceID().String())
		traceFlags := zap.Int("traceflags", int(context.TraceFlags()))
		allFields = append(allFields, []zap.Field{spanField, traceField, traceFlags}...)
	}
	log.Logger.Info(msg, allFields...)
}

package goframework

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func newExporter(ctx context.Context) (trace.SpanExporter, error) {
	return otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint("otlp.nr-data.net:4317"),
		otlptracegrpc.WithHeaders(map[string]string{"api-key": "b2da9ea0819c4b92a7289ac1eb6f5eac63afNRAL"}),
	)
	// return otlptrace.New(
	// 	context.Background(),
	// 	otlptracegrpc.NewClient(
	// 		otlptracegrpc.WithHeaders(map[string]string{"api-key": "b2da9ea0819c4b92a7289ac1eb6f5eac63afNRAL"}),
	// 		otlptracegrpc.WithEndpoint("otlp.nr-data.net:4317"),
	// 	),
	// )
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("ExampleService"),
		),
	)

	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

func newResource() *resource.Resource {
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("fib"),
			semconv.ServiceVersion("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	return r
}

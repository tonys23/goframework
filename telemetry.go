package goframework

import (
	"context"
	"log"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func newExporter(ctx context.Context, endpoint string, apikey string) (sdktrace.SpanExporter, error) {
	if endpoint == "" || apikey == "" {
		return stdouttrace.New(
			stdouttrace.WithWriter(os.Stdout),
			stdouttrace.WithPrettyPrint(),
			stdouttrace.WithoutTimestamps(),
		)
	}

	return otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithHeaders(map[string]string{"api-key": apikey}),
	)
}
func newTraceProvider(exp sdktrace.SpanExporter, r *resource.Resource) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

func newResource(projectName string) *resource.Resource {

	version := os.Getenv("apiVersion")
	env := os.Getenv("envApi")

	if projectName == "" {
		projectName = "ProjectName"
	}

	if version == "" {
		version = "v1.0.0"
	}

	if env == "" {
		env = "development"
	}

	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(projectName),
			semconv.ServiceVersion(version),
			attribute.String("environment", env),
		),
	)
	return r
}

type GoTelemetry struct {
	ProjectName string
	Endpoint    string
	ApiKey      string
}

func NewTelemetry(projectName string, endpoint string, apiKey string) *GoTelemetry {
	return &GoTelemetry{Endpoint: endpoint, ApiKey: apiKey, ProjectName: projectName}
}

func (gt *GoTelemetry) run(gf *GoFramework) {
	ctx := context.Background()
	nr, err := newExporter(ctx, gt.Endpoint, gt.ApiKey)
	if err != nil {
		log.Panic(err)
	}

	gf.projectName = gt.ProjectName
	gf.traceProvider = newTraceProvider(nr, newResource(gf.projectName))
}

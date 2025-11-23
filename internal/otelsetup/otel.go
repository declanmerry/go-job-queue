package otelsetup

import (
	"context"
	"log"

	"go-job-queue/internal/version"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"          // API
	mSdk "go.opentelemetry.io/otel/sdk/metric" // SDK
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	stdouttrace "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
)

var (
	Meter              metric.Meter
	JobsCreatedCounter metric.Int64Counter
)

func InitOTel(ctx context.Context) (func(context.Context) error, error) {

	// ---------- RESOURCE ----------
	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("go-job-queue"),
			semconv.ServiceVersion(version.Version),
		),
	)
	if err != nil {
		return nil, err
	}

	// ---------- TRACING (stdout only) ----------
	traceExp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}
	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExp),
		trace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)

	// ---------- METRICS (OTLP HTTP) ----------
	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}

	meterProvider := mSdk.NewMeterProvider(
		mSdk.WithReader(mSdk.NewPeriodicReader(metricExp)),
		mSdk.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	// Build meter
	Meter = meterProvider.Meter("go-job-queue")

	// ---------- METRIC COUNTERS ----------
	JobsCreatedCounter, err = Meter.Int64Counter("jobs_created_total")
	if err != nil {
		return nil, err
	}

	log.Println("[OTEL] OTel tracing + metrics initialized")

	// ---------- SHUTDOWN ----------
	return func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return err
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			return err
		}
		return nil
	}, nil
}

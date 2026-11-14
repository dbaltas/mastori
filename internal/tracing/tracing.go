package tracing

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// SetupFromEnv configures tracing for serviceName using the standard
// OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_TRACES_SAMPLER_ARG variables. It is a
// no-op when the endpoint is unset.
func SetupFromEnv(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	ratio := 1.0
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err == nil && r >= 0 && r <= 1 {
			ratio = r
		}
	}
	return Setup(ctx, serviceName, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), ratio)
}

// InjectTraceContext serializes the current span context of ctx into the W3C
// traceparent/tracestate fields carried on a Job, so a consumer in another
// process can continue the same trace.
func InjectTraceContext(ctx context.Context) (traceparent, tracestate string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}

// ExtractTraceContext rebuilds a context carrying the remote span context
// from the serialized trace context fields of a Job. The returned context can
// be used as a parent for new spans, linking them into the original trace.
// An empty traceparent returns ctx unchanged.
func ExtractTraceContext(ctx context.Context, traceparent, tracestate string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{
		"traceparent": traceparent,
		"tracestate":  tracestate,
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func Setup(ctx context.Context, serviceName, endpoint string, samplerRatio float64) (func(context.Context) error, error) {
	if endpoint == "" {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	insecure := false
	target := endpoint
	if u, err := url.Parse(endpoint); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		insecure = u.Scheme == "http"
		target = u.Host + u.Path
		target = strings.TrimSuffix(target, "/")
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(target),
	}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(samplerRatio))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

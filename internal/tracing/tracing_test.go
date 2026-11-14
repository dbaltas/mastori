package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractTraceContextRoundTrip(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	traceparent, _ := InjectTraceContext(ctx)
	want := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if traceparent != want {
		t.Fatalf("traceparent = %q, want %q", traceparent, want)
	}

	extracted := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), traceparent, ""))
	if !extracted.IsValid() {
		t.Fatal("extracted span context invalid")
	}
	if extracted.TraceID() != tid || extracted.SpanID() != sid {
		t.Errorf("extracted = %v, want trace %s span %s", extracted, tid, sid)
	}
	if !extracted.IsRemote() {
		t.Error("extracted span context should be marked remote")
	}
}

func TestInjectTraceContextNoSpan(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceparent, _ := InjectTraceContext(context.Background())
	if traceparent != "" {
		t.Errorf("traceparent = %q, want empty for context without span", traceparent)
	}
}

func TestExtractTraceContextEmptyPassthrough(t *testing.T) {
	ctx := context.Background()
	if got := ExtractTraceContext(ctx, "", ""); got != ctx {
		t.Error("empty traceparent must return the context unchanged")
	}
}

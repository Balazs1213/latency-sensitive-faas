from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from typing import cast

def instrument_app(app_name: str, service_name: str) -> trace.Tracer:
    trace.set_tracer_provider(
        TracerProvider(resource=Resource.create({SERVICE_NAME: service_name, "app.name": app_name}))
    )

    otel_exporter = OTLPSpanExporter(endpoint="otel-collector.observability:4317", insecure=True)
    provider = cast(TracerProvider, trace.get_tracer_provider())
    provider.add_span_processor(
        BatchSpanProcessor(otel_exporter)
    )
    tracer = trace.get_tracer(__name__)
    return tracer



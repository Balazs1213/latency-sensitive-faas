import concurrent.futures
from parliament import Context  # type: ignore
import requests
from opentelemetry.propagate import inject, extract
import tracing
from opentelemetry import trace
from opentelemetry.context import Context as OtelContext
from config import APP_NAME, FUNCTION_NAME, read_config
from route import Route
from typing import Any, Dict, Deque, List, Tuple, Optional, TypedDict, Union
from collections import deque
from event import Event, extract_event, create_event
from opentelemetry.trace.status import Status, StatusCode  # Add this import
from logger import setup_logging
from results import write_result
import faulthandler
import sys
import time

faulthandler.enable(file=sys.stderr, all_threads=True)

MAX_FORWARD_WORKERS = 10
forward_executor: concurrent.futures.ThreadPoolExecutor = (
    concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FORWARD_WORKERS)
)

logger = setup_logging(__name__)
tracer = tracing.instrument_app(app_name=APP_NAME, service_name=FUNCTION_NAME)


class RouteToProcess(TypedDict):
    """
    Represents a route to be processed.
    """

    route: Route
    input: Event


def get_headers(
    component: str, span_context: Optional[OtelContext] = None, correlation_id: str = ""
) -> Dict[str, str]:
    """
    Generates headers for the next component, including trace context.
    """
    current_time_epoch = f"{time.time_ns()}"
    headers = {
        "X-Forward-To": component,
        "Content-Type": "application/json",
        QUEUE_START_TIME_HEADER: current_time_epoch,
        CORRELATION_ID_HEADER: correlation_id,
    }

    if span_context:
        inject(headers, context=span_context)
    return headers


# Handler can return event out as a dictionary, or bytes, or even as a list of dicts/bytes
# A list signals multiple outgoing invocations
HandlerReturnType = Union[Dict[str, Any], bytes, List[Dict[str, Any]], List[bytes]]


def _handler_worker(component: str, event: Event) -> Any:
    """
    Worker function executed in a separate process.
    Dynamically imports func.HANDLERS to avoid stale global state.
    """
    from config import HANDLERS

    try:
        if component not in HANDLERS:
            raise ValueError(f"No handler registered for component '{component}'")

        handler = HANDLERS[component]
        return handler(event)
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e


def handle_request(
    event: Event, component: str, parent_context: Optional[OtelContext]
) -> Tuple[HandlerReturnType, Optional[OtelContext]]:
    """
    Handles the request by invoking the appropriate handler in a worker process.
    The handler is executed in an isolated process to prevent global tracer races.
    """
    with tracer.start_as_current_span(component, context=parent_context) as span:
        try:
            logger.info(f"Starting handler for component '{component}'.")

            # Submit the job to a worker process
            event_out = _handler_worker(component, event)

            logger.info(f"Component '{component}' processed successfully.")
            span.set_status(Status(StatusCode.OK))
            return event_out, trace.set_span_in_context(span)

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            logger.error(f"Error processing component '{component}': {e}")
            raise e


FIRE_AND_FORGET_TIMEOUT = (5.0, 0.01)


def _send_async_request(
    route: Route,
    event_out: Any,
    span_context: Optional[OtelContext],
    correlation_id: str = "",
) -> None:
    headers = get_headers(route["component"], span_context, correlation_id)
    with tracer.start_span("forward_request", context=span_context) as span:
        try:
            requests.post(
                url=route["url"],
                json=event_out,
                headers=headers,
                timeout=FIRE_AND_FORGET_TIMEOUT,
            )
            logger.info(
                f"Request forwarded to component '{route['component']}' at URL '{route['url']}'."
            )
            span.set_status(Status(StatusCode.OK))
        except requests.exceptions.ReadTimeout:
            # THIS IS THE EXPECTED OUTCOME.
            # We treat this as a success for our "fire-and-forget" model.
            logger.info(
                f"Request forwarded (fire-and-forget) to '{route['component']}' at '{route['url']}'. Read timeout occurred as expected."
            )
            span.set_status(
                Status(StatusCode.OK)
            )  # Set status to OK, this was intended.
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            logger.error(
                f"Error forwarding request to '{route['component']}' at '{route['url']}': {e}"
            )
            raise e


def forward_request(
    forward_futures: List[concurrent.futures.Future[None]],
    route: Route,
    event_out: Any,
    span_context: Optional[OtelContext],
    correlation_id: str = "",
) -> None:
    """
    Asynchronously forwards the request to the next component if there is one.
    """
    if not route["component"]:
        logger.warning("Attempted to forward request with empty component.")
        return

    future: concurrent.futures.Future[None] = forward_executor.submit(
        _send_async_request, route, event_out, span_context, correlation_id
    )
    forward_futures.append(future)


QUEUE_START_TIME_HEADER = "X-Request-Start-Time"
CORRELATION_ID_HEADER = "X-Correlation-ID"
TRACE_BOUNDARY_START_LABEL = "trace_boundary_start"
TRACE_BOUNDARY_END_LABEL = "trace_boundary_end"


def main(context: Context) -> Tuple[str, int]:
    """
    Entry point of the function. Processes the routing table using parallel processing.
    """
    logger.info("Headers received: " + str(context.request.headers))

    forward_futures: List[concurrent.futures.Future[Any]] = []

    correlation_id = context.request.headers.get(CORRELATION_ID_HEADER, "")

    component = context.request.headers.get("X-Forward-To")
    if not component:
        logger.error(f"No component found with name {component}")
        return f"No component found with name {component}", 400

    processing_queue: Deque[RouteToProcess] = deque(
        [
            RouteToProcess(
                route=Route(component=component, url="local"),
                input=extract_event(context),
            )
        ]
    )
    span_context: Optional[OtelContext] = extract(context.request.headers)
    is_trace_start = not bool(
        span_context
        and trace.get_current_span(span_context).get_span_context().is_valid
    )

    if is_trace_start:
        with tracer.start_as_current_span(
            "workflow",
        ) as workflow_span:
            span_context = trace.set_span_in_context(workflow_span)

    queue_start_epoch_str = context.request.headers.get(QUEUE_START_TIME_HEADER)
    logger.info(
        f"Queue start time header '{QUEUE_START_TIME_HEADER}': {queue_start_epoch_str}"
    )

    try:
        queue_start_time_ns = int(queue_start_epoch_str)
        queue_end_time_ns = time.time_ns()

        attributes: Dict[str, bool] = {}
        if is_trace_start:
            attributes = {
                TRACE_BOUNDARY_START_LABEL: True,
            }

        queue_span = tracer.start_span(
            name="queue",
            context=span_context,
            start_time=queue_start_time_ns,
        )

        queue_span.set_attributes(attributes)
        span_context = trace.set_span_in_context(queue_span)

        queue_span.end(end_time=queue_end_time_ns)
        logger.info(
            f"'queue' span created. Queue time: {(queue_end_time_ns - queue_start_time_ns)/1_000_000:.3f}ms"
        )
    except ValueError as e:
        logger.error(f"Could not parse queue start time '{queue_start_epoch_str}': {e}")
    except Exception as e:
        logger.error(f"Error creating 'queue' span: {e}")

    # Read routing table from redis
    with tracer.start_as_current_span("read_config", context=span_context) as span:
        try:
            routing_table = read_config()
            logger.info("Routing table read from Redis successfully.")
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            logger.error(f"Error reading routing table from Redis: {e}")
            return "Error: Routing table could not be read from Redis", 500

    try:
        while processing_queue:
            current = processing_queue.popleft()
            component = current["route"]["component"]

            try:
                output, span_context = handle_request(
                    current["input"], component, span_context
                )
            except Exception:
                logger.error(f"Error in component '{component}', aborting workflow.")
                return f"Error processing component '{component}'", 500

            outputs = output if isinstance(output, list) else [output]
            for o in outputs:
                next_routes = routing_table.get(component, [])
                if not next_routes:
                    # No next components, write result to Redis
                    with tracer.start_as_current_span(
                        "write_result",
                        context=span_context,
                        attributes={TRACE_BOUNDARY_END_LABEL: True},
                    ) as span:
                        try:
                            write_result(o, correlation_id)
                            logger.info(
                                f"Result for component '{component}' written to Redis."
                            )
                            span.set_status(Status(StatusCode.OK))
                        except Exception as e:
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.record_exception(e)
                            logger.error(
                                f"Error writing result for component '{component}': {e}"
                            )
                            return (
                                f"Error writing result for component '{component}'",
                                500,
                            )
                for next_route in next_routes:
                    if next_route["url"] == "local":
                        event_in = create_event(o)
                        route_to_process = RouteToProcess(
                            route=next_route, input=event_in
                        )
                        processing_queue.append(route_to_process)
                    else:
                        forward_request(
                            forward_futures, next_route, o, span_context, correlation_id
                        )
    except KeyError as e:
        logger.error(f"Invalid routing table entry: {e} is missing")
        return f"Invalid routing table entry: {e} is missing", 400

    if forward_futures:
        done, _ = concurrent.futures.wait(forward_futures, timeout=0.01)
        # Check for exceptions in the completed futures
        for future in done:
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred in a forwarded request thread: {e}")
                return "Error in forwarded request: " + str(e), 500

    logger.info("All components processed successfully.")
    return "ok", 200

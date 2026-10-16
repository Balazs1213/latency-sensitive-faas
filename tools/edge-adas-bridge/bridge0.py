import os
import uuid

import pika
import requests

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@lavinmq:5672/%2F")
INPUT_QUEUE = os.getenv("INPUT_QUEUE", "camera-stream")
STREAM_FILTER = os.getenv("STREAM_FILTER", "camera-2d")
TARGET_URL = os.environ["TARGET_URL"]
FORWARD_TO = os.getenv("FORWARD_TO", "detect-track-service")


def on_message(ch, method, properties, body):
    """Forward one camera frame to the framework app, then ack."""
    headers = {
        "Content-Type": "application/json",
        "X-Forward-To": FORWARD_TO,
        "X-Correlation-ID": f"bridge0-{uuid.uuid4().hex[:12]}",
    }
    try:
        r = requests.post(TARGET_URL, data=body, headers=headers, timeout=30)
        print(f"forwarded {len(body)} bytes -> HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"forward failed: {e}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)  # one frame at a time keeps per-camera order
    ch.basic_consume(
        queue=INPUT_QUEUE,
        on_message_callback=on_message,
        arguments={"x-stream-offset": "next", "x-stream-filter": STREAM_FILTER},
    )
    print(f"bridge0: {INPUT_QUEUE} ({STREAM_FILTER}) -> {TARGET_URL}", flush=True)
    ch.start_consuming()


if __name__ == "__main__":
    main()
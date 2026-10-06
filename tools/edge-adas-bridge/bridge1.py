"""Bridge 1: detection-tracking-results (LavinMQ) -> position-service (HTTP)."""
import os
import uuid

import pika
import requests

# Settings come from environment variables, so the same code runs anywhere.
AMQP_URL = os.environ["AMQP_URL"]
POSITION_URL = os.environ["POSITION_URL"]
QUEUE = "detection-tracking-results"


def on_message(channel, method, properties, body):
    # A short unique id so one message can be followed through the whole chain.
    cid = "bridge-" + uuid.uuid4().hex[:12]
    try:
        resp = requests.post(
            POSITION_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Forward-To": "position-service",
                "X-Correlation-ID": cid,
            },
            timeout=5,
        )
        print(f"{cid} -> HTTP {resp.status_code}", flush=True)
    except requests.RequestException as exc:
        print(f"{cid} -> ERROR {exc}", flush=True)
    finally:
        # Always ack: messages expire after 1 s anyway, redelivery is useless.
        channel.basic_ack(delivery_tag=method.delivery_tag)



def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    # passive=True: use the existing queue as is, never change its settings.
    channel.queue_declare(queue=QUEUE, passive=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print(f"Consuming {QUEUE}", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
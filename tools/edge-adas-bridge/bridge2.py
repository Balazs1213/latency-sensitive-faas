"""Bridge 2: result:<app id> (Redis) -> fusion-results-stream (LavinMQ)."""
import json
import os
import time

import pika
import redis

# Settings come from environment variables.
AMQP_URL = os.environ["AMQP_URL"]
REDIS_HOST = os.environ["REDIS_HOST"]
RESULT_KEY = os.environ["RESULT_KEY"]
QUEUE = "fusion-results-stream"
POLL_INTERVAL_S = 0.2

def open_channel():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    # passive=True: the stream already exists, never change its settings.
    channel.queue_declare(queue=QUEUE, passive=True)
    return connection, channel


def publish(channel, event):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(event).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={"x-stream-filter-value": event.get("vehicle_id", "")},
        ),
    )

def main():
    rds = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    connection, channel = open_channel()

    # Remember what is already in the list, so old results are not replayed.
    seen = {json.loads(raw)["timestamp"] for raw in rds.lrange(RESULT_KEY, 0, -1)}
    print(f"Watching {RESULT_KEY}, skipping {len(seen)} old entries", flush=True)

    while True:
        # The list keeps only the last 10 entries, oldest first.
        entries = [json.loads(raw) for raw in rds.lrange(RESULT_KEY, 0, -1)]
        for entry in entries:
            key = entry["timestamp"]
            if key in seen:
                continue
            seen.add(key)
            event = entry["event"]
            if "fused_object_count" not in event:
                print(f"{key} skipped: not a fusion result", flush=True)
                continue
            publish(channel, event)
            print(f"{key} published ({event['fused_object_count']} objects)", flush=True)
        # Forget ids that left the list, so the set stays small.
        seen &= {entry["timestamp"] for entry in entries}
        # Waits POLL_INTERVAL_S and keeps the LavinMQ connection alive.
        connection.process_data_events(time_limit=POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
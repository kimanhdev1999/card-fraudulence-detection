#!/usr/bin/env python

"""
Streaming AI consumer: reads transactions from Kafka and invokes an
inference hook per message. Optionally publishes predictions to a Kafka topic.
"""

import json
import signal
from argparse import ArgumentParser
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer


def run_inference(transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Replace this stub with your AI model inference.
    Return a dictionary to publish to the predictions topic, or None to skip.
    """
    return None


def main():
    parser = ArgumentParser(description="AI consumer: read transactions and run inference")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", type=str, help="Kafka bootstrap servers")
    parser.add_argument("--group-id", default="ai-consumer", type=str, help="Kafka consumer group id")
    parser.add_argument("--input-topic", default="transactions", type=str, help="Input topic with transactions")
    parser.add_argument("--predictions-topic", default=None, type=str, help="Optional topic to publish predictions")
    args = parser.parse_args()

    consumer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.input_topic])

    producer: Optional[Producer] = None
    if args.predictions_topic:
        producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    running = True

    def handle_sigint(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                print(f"Failed to decode JSON: {exc}")
                continue

            result = run_inference(payload)

            if result is not None and producer is not None and args.predictions_topic:
                try:
                    key = str(payload.get("id", "")).encode("utf-8")
                    producer.produce(args.predictions_topic, key=key, value=json.dumps(result).encode("utf-8"))
                    producer.poll(0)
                except Exception as exc:
                    print(f"Failed to produce prediction: {exc}")

    finally:
        try:
            if producer is not None:
                producer.flush(5)
        finally:
            consumer.close()


if __name__ == "__main__":
    main()

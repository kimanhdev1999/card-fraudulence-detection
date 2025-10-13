#!/usr/bin/env python

"""
AI Consumer microservice for local development.
Consumes Kafka topic "transactions", runs inference, and optionally publishes
predictions to another Kafka topic.
"""

import json
import os
import signal
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer


def run_inference(transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Compute risk score and human-readable reason for a transaction."""
    try:
        amount = float(transaction.get("amount", 0))
    except Exception:
        amount = 0.0

    category = str(transaction.get("category", "")).lower()
    city = str(transaction.get("city", "")).lower()
    state = str(transaction.get("state", "")).lower()
    gender = str(transaction.get("gender", "")).lower()

    signals = []

    if amount >= 1000:
        signals.append((0.6, f"High amount ${amount:.2f}"))
    elif amount >= 500:
        signals.append((0.35, f"Elevated amount ${amount:.2f}"))

    risky_categories = {"electronics", "jewelry", "luxury", "crypto"}
    if category in risky_categories:
        signals.append((0.25, f"Risky category '{category}'"))

    unusual_locales = {"nv", "pr", "vi", "ak"}
    if state in unusual_locales:
        signals.append((0.15, f"Unusual state '{state}'"))

    if gender not in {"male", "female"}:
        signals.append((0.05, "Missing/unknown gender"))

    # Combine signals into score (cap at 0.99)
    score = min(0.99, sum(w for w, _ in signals)) if signals else 0.02

    # Determine reasons (top 2)
    reasons = [reason for _, reason in sorted(signals, key=lambda x: x[0], reverse=True)[:2]]
    if not reasons and score <= 0.05:
        reasons = ["Low risk baseline"]

    flagged = score >= 0.8

    return {
        "id": transaction.get("id"),
        "score": round(score, 3),
        "flagged": flagged,
        "reasons": reasons,
        "category": category,
        "amount": amount,
        "city": city,
        "state": state,
    }


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", "broker1:9092")
    group_id = os.getenv("GROUP_ID", "ai-consumer")
    input_topic = os.getenv("INPUT_TOPIC", "transactions")
    predictions_topic = os.getenv("PREDICTIONS_TOPIC", "predictions")

    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([input_topic])

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

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
                print(f"Invalid JSON: {exc}")
                continue

            result = run_inference(payload)
            if result is None:
                continue
            try:
                key = str(payload.get("id", "")).encode("utf-8")
                producer.produce(predictions_topic, key=key, value=json.dumps(result).encode("utf-8"))
                producer.poll(0)
            except Exception as exc:
                print(f"Failed to produce prediction: {exc}")

    finally:
        try:
            producer.flush(5)
        finally:
            consumer.close()


if __name__ == "__main__":
    main()



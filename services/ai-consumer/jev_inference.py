"""
Jev-based fraud scoring.

Uses TypeSafe AI's Jev model (a "System One" model) to turn an unstructured
transaction description into a typed, calibrated risk decision instead of
hand-written heuristics or free-text LLM output.

Docs: https://docs.typesafe.ai/
"""

import os
from typing import Any, Dict, Optional

from typesafe_sdk import Choice, Score, TypeSafeClient

RISK_LEVELS = [
    "Not risky: ordinary, low-value, routine transaction",
    "Slightly risky: mildly unusual amount or category",
    "Risky: elevated amount, risky category, or unusual location",
    "Very risky: strong combination of signals suggesting fraud",
]

CATEGORY_OPTIONS = {
    "amount": "The amount of the transaction is the primary risk driver",
    "category": "The merchant category is the primary risk driver",
    "location": "The customer's location is the primary risk driver",
    "profile": "The customer's profile (gender/job/other) is the primary risk driver",
    "none": "No single factor stands out as the primary risk driver",
}

_client: Optional[TypeSafeClient] = None


def _get_client() -> TypeSafeClient:
    global _client
    if _client is None:
        api_key = os.environ["TYPESAFE_API_KEY"]
        _client = TypeSafeClient(api_key=api_key, timeout=float(os.getenv("TYPESAFE_TIMEOUT", "30")))
    return _client


def _describe_transaction(transaction: Dict[str, Any]) -> str:
    amount = transaction.get("amount", 0)
    category = transaction.get("category", "unknown")
    city = transaction.get("city", "unknown")
    state = transaction.get("state", "unknown")
    gender = transaction.get("gender", "unknown")
    job = transaction.get("job", "unknown")
    return (
        f"Transaction amount: ${float(amount):.2f}. "
        f"Category: {category}. "
        f"Customer location: {city}, {state}. "
        f"Customer gender: {gender}. Customer job: {job}."
    )


def run_inference(transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Score a transaction for fraud risk using the Jev model."""
    client = _get_client()
    state = _describe_transaction(transaction)

    response = client.system_one(
        state=state,
        questions={
            "risk": Score(
                instructions="How risky is this credit card transaction for fraud?",
                criteria=RISK_LEVELS,
            ),
            "driver": Choice(
                instructions="What is the primary driver of the risk level?",
                criteria=CATEGORY_OPTIONS,
            ),
        },
        model=os.getenv("TYPESAFE_MODEL", "jev-latest"),
    )

    risk_answer = response.answers["risk"]
    driver_answer = response.answers["driver"]

    # Normalize the rubric-level score (0..len(RISK_LEVELS)-1) to a 0..1 score.
    score = round(risk_answer.score / (len(RISK_LEVELS) - 1), 3)
    flagged = score >= 0.8

    reasons = [
        f"Jev risk level: {risk_answer.legend.get(round(risk_answer.score), '')}",
        f"Primary driver: {driver_answer.choice} (confidence {driver_answer.confidence:.2f})",
    ]

    return {
        "id": transaction.get("id"),
        "score": score,
        "flagged": flagged,
        "reasons": reasons,
        "category": transaction.get("category"),
        "amount": transaction.get("amount"),
        "city": transaction.get("city"),
        "state": transaction.get("state"),
    }

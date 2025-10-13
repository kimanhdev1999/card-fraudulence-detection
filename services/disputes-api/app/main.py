from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import uuid
import json
import os


DATA_FILE = os.getenv("DATA_FILE", "/data/disputes.json")

app = FastAPI(title="Disputes API", version="0.1.0")


class TimelineEvent(BaseModel):
    ts: datetime
    status: str
    note: Optional[str] = None


class Dispute(BaseModel):
    id: str
    transaction_id: str
    customer_id: Optional[str] = None
    description: str
    status: str = Field(default="submitted")
    chargeback_code: Optional[str] = None
    timeline: List[TimelineEvent] = Field(default_factory=list)


class DisputeCreate(BaseModel):
    transaction_id: str
    customer_id: Optional[str] = None
    description: str


class SuggestRequest(BaseModel):
    description: str


def _load() -> List[Dispute]:
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return [Dispute(**d) for d in raw]
    except FileNotFoundError:
        return []


def _save(items: List[Dispute]) -> None:
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump([json.loads(i.json()) for i in items], f, ensure_ascii=False, indent=2)


def suggest_chargeback_code(description: str) -> str:
    text = description.lower()
    # Simple heuristic mapping; replace with ML if available
    if any(k in text for k in ["card not present", "cnp", "fraud", "unauthorized"]):
        return "10.4"  # Fraud: Other (Visa example)
    if any(k in text for k in ["not received", "never arrived", "no delivery"]):
        return "13.1"  # Merchandise/Services Not Received
    if any(k in text for k in ["not as described", "defective", "broken", "damaged"]):
        return "13.3"  # Not as Described or Defective Merchandise
    if any(k in text for k in ["duplicate", "charged twice", "double charge"]):
        return "12.6"  # Duplicate Processing
    if any(k in text for k in ["credit not processed", "refund not received"]):
        return "13.6"  # Credit Not Processed
    return "13.9"  # Miscellaneous / General


@app.post("/disputes", response_model=Dispute)
def create_dispute(req: DisputeCreate):
    items = _load()
    did = str(uuid.uuid4())
    code = suggest_chargeback_code(req.description)
    now = datetime.utcnow()
    dispute = Dispute(
        id=did,
        transaction_id=req.transaction_id,
        customer_id=req.customer_id,
        description=req.description,
        status="submitted",
        chargeback_code=code,
        timeline=[TimelineEvent(ts=now, status="submitted", note="Dispute created")],
    )
    items.append(dispute)
    _save(items)
    return dispute


@app.get("/disputes/{dispute_id}", response_model=Dispute)
def get_dispute(dispute_id: str):
    items = _load()
    for d in items:
        if d.id == dispute_id:
            return d
    raise HTTPException(status_code=404, detail="Dispute not found")


@app.get("/disputes", response_model=List[Dispute])
def list_disputes():
    return _load()


class UpdateStatus(BaseModel):
    status: str
    note: Optional[str] = None


@app.post("/disputes/{dispute_id}/status", response_model=Dispute)
def update_status(dispute_id: str, upd: UpdateStatus):
    items = _load()
    for i, d in enumerate(items):
        if d.id == dispute_id:
            d.status = upd.status
            d.timeline.append(TimelineEvent(ts=datetime.utcnow(), status=upd.status, note=upd.note))
            items[i] = d
            _save(items)
            return d
    raise HTTPException(status_code=404, detail="Dispute not found")


@app.post("/disputes/suggest-code")
def suggest_code(req: SuggestRequest):
    return {"code": suggest_chargeback_code(req.description)}



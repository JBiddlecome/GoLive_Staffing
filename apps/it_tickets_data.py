import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_ROOT / "data" / "it_tickets.json"


def _ensure_data_file() -> None:
    if not DATA_FILE.exists():
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps({"tickets": []}, indent=2))


def load_tickets() -> list:
    _ensure_data_file()
    with DATA_FILE.open() as f:
        return json.load(f).get("tickets", [])


def save_ticket(*, submitted_by: str, department: str, request_details: str, priority: str) -> None:
    _ensure_data_file()
    with DATA_FILE.open() as f:
        data = json.load(f)
    data.setdefault("tickets", []).append({
        "id": str(uuid.uuid4()),
        "submitted_by": submitted_by,
        "department": department,
        "request_details": request_details,
        "priority": priority,
        "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "estimated_completion": None,
    })
    DATA_FILE.write_text(json.dumps(data, indent=2))


def set_ticket_estimate(ticket_id: str, estimated_completion: str | None) -> bool:
    _ensure_data_file()
    with DATA_FILE.open() as f:
        data = json.load(f)
    for ticket in data.get("tickets", []):
        if ticket["id"] == ticket_id:
            ticket["estimated_completion"] = estimated_completion or None
            DATA_FILE.write_text(json.dumps(data, indent=2))
            return True
    return False

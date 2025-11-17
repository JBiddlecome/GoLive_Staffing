from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

DATA_FILE = Path("data/concierge_records.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

REQUIRED_COLUMNS = [
    "Employee ID",
    "Status",
    "First Name",
    "Last Name",
    "Start Date",
    "Rehire Date",
    "Concierge Date",
]

ALLOWED_RECRUITERS = ["Piyush", "Prafull", "Anmol", "Christina"]
START_DATE_CUTOFF = pd.Timestamp(year=2025, month=10, day=1)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def concierge_page(
    request: Request,
    concierged: str = Query("all"),
    notice: str = Query(""),
    error: str = Query(""),
) -> HTMLResponse:
    return _render_page(request, concierged_filter=concierged, notice=notice, error=error)


@router.post("/upload", response_class=HTMLResponse)
async def upload_concierge_file(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    file_bytes = await file.read()
    if not file_bytes:
        return _render_page(request, error="The uploaded file was empty.")

    try:
        dataframe = _load_dataframe(file_bytes, file.filename)
    except ValueError as exc:
        return _render_page(request, error=str(exc))

    try:
        added = _merge_new_employees(dataframe)
    except ValueError as exc:
        return _render_page(request, error=str(exc))

    notice = f"Added {added} new employee(s) from the upload."
    return _render_page(request, notice=notice)


@router.post("/update", response_class=HTMLResponse)
async def update_employee(
    request: Request,
    employee_id: str = Form(...),
    called_date: str = Form(""),
    call_count: str = Form(""),
    recruiter: str = Form(""),
    concierged: str | None = Form(None),
    concierged_filter: str = Form("all"),
) -> HTMLResponse:
    records = _load_records()
    existing = {record.get("employee_id"): record for record in records}

    if employee_id not in existing:
        return _render_page(request, error="Employee not found.", concierged_filter=concierged_filter)

    record = existing[employee_id]

    normalized_called = _normalize_date_str(called_date)
    if called_date and not normalized_called:
        return _render_page(
            request,
            error="Called Date must be a valid date (YYYY-MM-DD).",
            concierged_filter=concierged_filter,
        )

    try:
        parsed_calls = int(call_count) if str(call_count).strip() else 0
        if parsed_calls < 0:
            raise ValueError
    except ValueError:
        return _render_page(
            request,
            error="Number of calls must be a non-negative integer.",
            concierged_filter=concierged_filter,
        )

    record["called_date"] = normalized_called
    record["call_count"] = parsed_calls
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS else ""
    record["concierged"] = concierged is not None

    _save_records(records)

    return _render_page(request, notice="Employee updated.", concierged_filter=concierged_filter)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _render_page(
    request: Request,
    *,
    concierged_filter: str = "all",
    notice: str = "",
    error: str = "",
) -> HTMLResponse:
    records = _load_records()
    filtered_records = _apply_concierged_filter(records, concierged_filter)

    concierged_count = sum(1 for record in records if record.get("concierged"))
    context: Dict[str, object] = {
        "request": request,
        "records": filtered_records,
        "concierged_filter": concierged_filter,
        "notice": notice,
        "error": error,
        "summary": {
            "total": len(records),
            "concierged": concierged_count,
        },
        "recruiters": ALLOWED_RECRUITERS,
    }

    status_code = 400 if error else 200
    return templates.TemplateResponse("apps/concierge.html", context, status_code=status_code)


def _apply_concierged_filter(records: List[Dict[str, object]], filter_value: str) -> List[Dict[str, object]]:
    normalized = (filter_value or "all").lower()
    if normalized == "yes":
        return [record for record in records if record.get("concierged")]
    if normalized == "no":
        return [record for record in records if not record.get("concierged")]
    return records


def _load_dataframe(file_bytes: bytes, filename: str) -> pd.DataFrame:
    buffer = io.BytesIO(file_bytes)
    try:
        if filename.lower().endswith(".csv"):
            dataframe = pd.read_csv(buffer)
        else:
            dataframe = pd.read_excel(buffer)
    except Exception as exc:  # pragma: no cover - pandas specific
        raise ValueError(f"Could not read file: {exc}") from exc

    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Uploaded file is missing required columns: {missing_list}.")

    return dataframe


def _merge_new_employees(dataframe: pd.DataFrame) -> int:
    records = _load_records()
    existing_ids = {record.get("employee_id") for record in records}
    added = 0

    for _, row in dataframe.iterrows():
        status = str(row.get("Status", "")).strip().lower()
        if status != "active":
            continue

        start_date = _normalize_date(row.get("Start Date"))
        rehire_date = _normalize_date(row.get("Rehire Date"))

        if not _passes_date_cutoff(start_date, rehire_date):
            continue

        employee_id = str(row.get("Employee ID", "")).strip()
        if not employee_id or employee_id in existing_ids:
            continue

        concierge_date = _normalize_date(row.get("Concierge Date"))

        record = {
            "employee_id": employee_id,
            "first_name": str(row.get("First Name", "")).strip(),
            "last_name": str(row.get("Last Name", "")).strip(),
            "start_date": _format_date(start_date),
            "rehire_date": _format_date(rehire_date),
            "concierge_date": _format_date(concierge_date),
            "called_date": "",
            "call_count": 0,
            "recruiter": "",
            "concierged": concierge_date is not None,
        }

        records.append(record)
        existing_ids.add(employee_id)
        added += 1

    _save_records(records)
    return added


def _passes_date_cutoff(start_date: pd.Timestamp | None, rehire_date: pd.Timestamp | None) -> bool:
    return any(
        date_value is not None and date_value >= START_DATE_CUTOFF
        for date_value in (start_date, rehire_date)
    )


def _normalize_date(value) -> pd.Timestamp | None:
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:  # pragma: no cover - pandas specific
        return None

    if pd.isna(parsed):
        return None

    return parsed.normalize()


def _normalize_date_str(value: str) -> str:
    parsed = _normalize_date(value)
    return _format_date(parsed)


def _format_date(value: pd.Timestamp | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d")


def _load_records() -> List[Dict[str, object]]:
    if not DATA_FILE.exists():
        return []

    try:
        return json.loads(DATA_FILE.read_text())
    except json.JSONDecodeError:
        return []


def _save_records(records: List[Dict[str, object]]) -> None:
    DATA_FILE.write_text(json.dumps(records, indent=2))

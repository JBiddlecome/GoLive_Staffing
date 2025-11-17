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
_SORT_FIELDS = {
    "employee_id_asc": ("employee_id", False),
    "employee_id_desc": ("employee_id", True),
}

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def concierge_page(
    request: Request,
    concierged: str = Query("all"),
    sort: str = Query("employee_id_asc"),
    notice: str = Query(""),
    error: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
) -> HTMLResponse:
    return _render_page(
        request,
        concierged_filter=concierged,
        sort=sort,
        notice=notice,
        error=error,
        start_date=start_date,
        end_date=end_date,
    )


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
    sort: str = Form("employee_id_asc"),
    start_date: str = Form(""),
    end_date: str = Form(""),
) -> HTMLResponse:
    records = _load_records()
    existing = {record.get("employee_id"): record for record in records}

    if employee_id not in existing:
        return _render_page(
            request,
            error="Employee not found.",
            concierged_filter=concierged_filter,
            sort=sort,
        )

    record = existing[employee_id]

    normalized_called = _normalize_date_str(called_date)
    if called_date and not normalized_called:
        return _render_page(
            request,
            error="Called Date must be a valid date (YYYY-MM-DD).",
            concierged_filter=concierged_filter,
            sort=sort,
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
            sort=sort,
        )

    record["called_date"] = normalized_called
    record["call_count"] = parsed_calls
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS else ""
    record["concierged"] = concierged is not None

    _save_records(records)

    return _render_page(
        request,
        notice="Employee updated.",
        concierged_filter=concierged_filter,
        sort=sort,
        start_date=start_date,
        end_date=end_date,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _render_page(
    request: Request,
    *,
    concierged_filter: str = "all",
    sort: str = "employee_id_asc",
    notice: str = "",
    error: str = "",
    start_date: str = "",
    end_date: str = "",
) -> HTMLResponse:
    records = _load_records()
    normalized_sort = _normalize_sort(sort)

    parsed_start, parsed_end, date_error = _normalize_filter_dates(start_date, end_date)

    filter_error = date_error or ""
    filtered_records = _apply_concierged_filter(records, concierged_filter)
    if not date_error:
        filtered_records = _apply_date_range_filter(filtered_records, parsed_start, parsed_end)

    sorted_records = _apply_sort(filtered_records, normalized_sort)

    concierged_count = sum(1 for record in records if record.get("concierged"))
    combined_error = " ".join(msg for msg in (error, filter_error) if msg).strip()

    context: Dict[str, object] = {
        "request": request,
        "records": sorted_records,
        "concierged_filter": concierged_filter,
        "sort": normalized_sort,
        "notice": notice,
        "error": combined_error,
        "summary": {
            "total": len(records),
            "concierged": concierged_count,
        },
        "recruiters": ALLOWED_RECRUITERS,
        "start_date": start_date,
        "end_date": end_date,
    }

    status_code = 400 if combined_error else 200
    return templates.TemplateResponse("apps/concierge.html", context, status_code=status_code)


def _apply_concierged_filter(records: List[Dict[str, object]], filter_value: str) -> List[Dict[str, object]]:
    normalized = (filter_value or "all").lower()
    if normalized == "yes":
        return [record for record in records if record.get("concierged")]
    if normalized == "no":
        return [record for record in records if not record.get("concierged")]
    return records


def _apply_date_range_filter(
    records: List[Dict[str, object]],
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> List[Dict[str, object]]:
    if start_date is None and end_date is None:
        return records

    def _in_range(date_str: str) -> bool:
        parsed = _normalize_date(date_str)
        if parsed is None:
            return False
        if start_date is not None and parsed < start_date:
            return False
        if end_date is not None and parsed > end_date:
            return False
        return True

    return [
        record
        for record in records
        if _in_range(record.get("start_date", ""))
        or _in_range(record.get("rehire_date", ""))
    ]


def _apply_sort(records: List[Dict[str, object]], sort: str) -> List[Dict[str, object]]:
    field, reverse = _SORT_FIELDS.get(sort, _SORT_FIELDS["employee_id_asc"])
    return sorted(records, key=lambda record: record.get(field, ""), reverse=reverse)


def _normalize_sort(sort: str) -> str:
    normalized = (sort or "").lower()
    if normalized in _SORT_FIELDS:
        return normalized
    return "employee_id_asc"


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


def _normalize_filter_dates(
    start_date: str, end_date: str
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str | None]:
    parsed_start = _normalize_date(start_date) if start_date else None
    parsed_end = _normalize_date(end_date) if end_date else None

    if start_date and parsed_start is None:
        return None, None, "Start date must be a valid date (YYYY-MM-DD)."

    if end_date and parsed_end is None:
        return None, None, "End date must be a valid date (YYYY-MM-DD)."

    if parsed_start and parsed_end and parsed_start > parsed_end:
        return None, None, "Start date cannot be after end date."

    return parsed_start, parsed_end, None


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

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Callable, Dict, List

import pandas as pd
from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
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
    "Mobile",
    "Language",
]

ALLOWED_RECRUITERS = ["Piyush", "Prafull", "Anmol", "Christina"]
FOLLOW_UP_OPTIONS = {
    "reached_out_lvm": {"label": "Reached Out - LVM", "row_class": "bg-yellow-100"},
    "need_to_reach_out": {"label": "Need to reach out", "row_class": "bg-orange-100"},
    "ccd": {"label": "CC'd", "row_class": "bg-green-100"},
    "cancelled": {"label": "Cancelled/Termed not approved", "row_class": "bg-red-100"},
    "texted": {"label": "Texted", "row_class": "bg-blue-100"},
    "sw_calling_back": {"label": "SW Calling back", "row_class": "bg-white"},
    "last_reach_out_mass_text": {"label": "Last reach out mass text", "row_class": "bg-purple-100"},
}
START_DATE_CUTOFF = pd.Timestamp(year=2025, month=10, day=1)
_SORT_FIELDS = {
    "employee_id_asc": (lambda record: str(record.get("employee_id", "")).lower(), False),
    "employee_id_desc": (lambda record: str(record.get("employee_id", "")).lower(), True),
    "follow_up_status_asc": (
        lambda record: FOLLOW_UP_OPTIONS.get(record.get("follow_up_status", ""), {})
        .get("label", "")
        .lower(),
        False,
    ),
    "follow_up_status_desc": (
        lambda record: FOLLOW_UP_OPTIONS.get(record.get("follow_up_status", ""), {})
        .get("label", "")
        .lower(),
        True,
    ),
    "recruiter_asc": (lambda record: str(record.get("recruiter", "")).lower(), False),
    "recruiter_desc": (lambda record: str(record.get("recruiter", "")).lower(), True),
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
    follow_up_status: str = Form(""),
    concierged_filter: str = Form("all"),
    sort: str = Form("employee_id_asc"),
    start_date: str = Form(""),
    end_date: str = Form(""),
) -> HTMLResponse:
    records = _load_records()
    existing = {record.get("employee_id"): record for record in records}

    if employee_id not in existing:
        return _response_for_update(
            request,
            error="Employee not found.",
            concierged_filter=concierged_filter,
            sort=sort,
        )

    record = existing[employee_id]

    normalized_called = _normalize_date_str(called_date)
    if called_date and not normalized_called:
        return _response_for_update(
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
        return _response_for_update(
            request,
            error="Number of calls must be a non-negative integer.",
            concierged_filter=concierged_filter,
            sort=sort,
        )

    normalized_status = follow_up_status if follow_up_status in FOLLOW_UP_OPTIONS else ""

    record["called_date"] = normalized_called
    record["call_count"] = parsed_calls
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS else ""
    record["concierged"] = concierged is not None
    record["follow_up_status"] = normalized_status

    _save_records(records)

    return _response_for_update(
        request,
        notice="Employee updated.",
        concierged_filter=concierged_filter,
        sort=sort,
        start_date=start_date,
        end_date=end_date,
        record=record,
        follow_up_status_class=FOLLOW_UP_OPTIONS.get(normalized_status, {}).get("row_class", ""),
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

    effective_start, effective_end = start_date, end_date
    if not start_date and not end_date:
        effective_start, effective_end = _current_week_range()

    parsed_start, parsed_end, date_error = _normalize_filter_dates(effective_start, effective_end)

    filter_error = date_error or ""
    filtered_records = _apply_concierged_filter(records, concierged_filter)
    if not date_error:
        filtered_records = _apply_date_range_filter(filtered_records, parsed_start, parsed_end)

    sorted_records = _apply_sort(filtered_records, normalized_sort)

    concierged_count = sum(1 for record in filtered_records if record.get("concierged"))
    combined_error = " ".join(msg for msg in (error, filter_error) if msg).strip()

    context: Dict[str, object] = {
        "request": request,
        "records": sorted_records,
        "concierged_filter": concierged_filter,
        "sort": normalized_sort,
        "notice": notice,
        "error": combined_error,
        "summary": {
            "total": len(filtered_records),
            "concierged": concierged_count,
        },
        "recruiters": ALLOWED_RECRUITERS,
        "start_date": effective_start,
        "end_date": effective_end,
        "follow_up_options": FOLLOW_UP_OPTIONS,
    }

    status_code = 400 if combined_error else 200
    return templates.TemplateResponse("apps/concierge.html", context, status_code=status_code)


def _is_json_request(request: Request) -> bool:
    accept = request.headers.get("accept", "").lower()
    requested_with = request.headers.get("x-requested-with", "").lower()
    return "application/json" in accept or requested_with == "fetch"


def _response_for_update(
    request: Request,
    *,
    notice: str = "",
    error: str = "",
    concierged_filter: str = "all",
    sort: str = "employee_id_asc",
    start_date: str = "",
    end_date: str = "",
    record: Dict[str, object] | None = None,
    follow_up_status_class: str = "",
):
    if _is_json_request(request):
        status_code = 400 if error else 200
        payload: Dict[str, object] = {"notice": notice, "error": error}
        if record is not None:
            payload["record"] = record
        if follow_up_status_class:
            payload["follow_up_status_class"] = follow_up_status_class
        return JSONResponse(payload, status_code=status_code)

    return _render_page(
        request,
        notice=notice,
        error=error,
        concierged_filter=concierged_filter,
        sort=sort,
        start_date=start_date,
        end_date=end_date,
    )


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
    key_func, reverse = _SORT_FIELDS.get(sort, _SORT_FIELDS["employee_id_asc"])
    return sorted(records, key=key_func, reverse=reverse)


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
    existing_by_id = {record.get("employee_id"): record for record in records}
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
        if not employee_id:
            continue

        concierge_date = _normalize_date(row.get("Concierge Date"))
        mobile = str(row.get("Mobile", "")).strip()
        language = str(row.get("Language", "")).strip()

        formatted_start = _format_date(start_date)
        formatted_rehire = _format_date(rehire_date)
        formatted_concierge = _format_date(concierge_date)

        if employee_id in existing_by_id:
            record = existing_by_id[employee_id]
            record["first_name"] = str(row.get("First Name", "")).strip() or record.get("first_name", "")
            record["last_name"] = str(row.get("Last Name", "")).strip() or record.get("last_name", "")
            record["start_date"] = formatted_start or record.get("start_date", "")
            record["rehire_date"] = formatted_rehire or record.get("rehire_date", "")

            if formatted_concierge:
                record["concierge_date"] = formatted_concierge
                record["concierged"] = True

            if mobile:
                record["mobile"] = mobile
            if language:
                record["language"] = language
            continue

        record = {
            "employee_id": employee_id,
            "first_name": str(row.get("First Name", "")).strip(),
            "last_name": str(row.get("Last Name", "")).strip(),
            "start_date": formatted_start,
            "rehire_date": formatted_rehire,
            "concierge_date": formatted_concierge,
            "called_date": "",
            "call_count": 0,
            "recruiter": "",
            "concierged": concierge_date is not None,
            "mobile": mobile,
            "language": language,
            "follow_up_status": "",
        }

        records.append(record)
        existing_by_id[employee_id] = record
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


def _current_week_range() -> tuple[str, str]:
    today = pd.Timestamp.today().normalize()
    start_of_week = today - pd.Timedelta(days=today.weekday())
    return _format_date(start_of_week), _format_date(today)


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
        raw_records = json.loads(DATA_FILE.read_text())
    except json.JSONDecodeError:
        return []

    normalized_records = []
    for record in raw_records:
        if isinstance(record, dict):
            normalized_records.append(_ensure_record_defaults(record))
    return normalized_records


def _save_records(records: List[Dict[str, object]]) -> None:
    DATA_FILE.write_text(json.dumps(records, indent=2))


def _ensure_record_defaults(record: Dict[str, object]) -> Dict[str, object]:
    defaults = {
        "called_date": "",
        "call_count": 0,
        "recruiter": "",
        "concierged": False,
        "follow_up_status": "",
        "mobile": "",
        "language": "",
    }
    for key, value in defaults.items():
        record.setdefault(key, value)
    return record

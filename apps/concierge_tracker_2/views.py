from __future__ import annotations

import io
import json
import os
import time
from pathlib import Path
from typing import Callable, Dict, List

import pandas as pd
from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

DATA_FILE = Path("data/concierge_records.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

LAST_SYNC_TIMES: dict[tuple[pd.Timestamp | None, pd.Timestamp | None], float] = {}
SYNC_INTERVAL = 15 * 60  # 15 minutes

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

ALLOWED_RECRUITERS = ["Piyush", "Anmol", "Rohit"]
FOLLOW_UP_OPTIONS = {
    "reached_out_lvm": {"label": "Reached Out - LVM", "row_class": "bg-yellow-100"},
    "need_to_reach_out": {"label": "Need to reach out", "row_class": "bg-orange-100"},
    "ccd": {"label": "CC'd", "row_class": "bg-green-100"},
    "cancelled": {"label": "Cancelled/Termed not approved", "row_class": "bg-red-100"},
    "texted": {"label": "Texted", "row_class": "bg-blue-100"},
    "sw_calling_back": {"label": "SW Calling back", "row_class": "bg-white"},
    "last_reach_out_mass_text": {"label": "Last reach out mass text", "row_class": "bg-purple-100"},
}
START_DATE_CUTOFF = pd.Timestamp(year=2023, month=1, day=1)
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


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "localhost")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    return URL.create(drivername="mysql+pymysql", username=user, password=password, host=host, port=port, database=name)


def _maybe_sync_from_db(start_date: str, end_date: str):
    effective_start, effective_end = start_date, end_date
    if not start_date and not end_date:
        effective_start, effective_end = _current_week_range()

    parsed_start, parsed_end, date_error = _normalize_filter_dates(effective_start, effective_end)
    if date_error:
        return

    global LAST_SYNC_TIMES
    now = time.time()
    key = (parsed_start, parsed_end)
    if now - LAST_SYNC_TIMES.get(key, 0) < SYNC_INTERVAL:
        return
    
    LAST_SYNC_TIMES[key] = now
    
    engine = create_engine(_db_url_from_env(), pool_pre_ping=True)
    try:
        sql = text("""
            SELECT 
                e.employee_id AS `DB Employee ID`,
                e.payroll_id AS `Employee ID`,
                IF(e.status = 1, 'active', 'inactive') AS `Status`,
                e.first_name AS `First Name`,
                e.last_name AS `Last Name`,
                e.start_date AS `Start Date`,
                e.start_date2 AS `Rehire Date`,
                e.mobile AS `Mobile`,
                GROUP_CONCAT(
                    CASE 
                        WHEN el.language_id = 10 THEN 'Spanish'
                        WHEN el.language_id = 23 THEN 'English'
                        WHEN el.language_id = 24 THEN 'French'
                        ELSE NULL
                    END
                    SEPARATOR ', '
                ) AS `Language`,
                (SELECT MAX(ev.date) FROM timesheet t JOIN event ev ON t.event_id = ev.event_id WHERE t.employee_id = e.employee_id) AS `Last Timesheet Date`
            FROM employee e
            LEFT JOIN employee_language el ON e.employee_id = el.employee_id
            WHERE e.payroll_id IS NOT NULL AND e.payroll_id != ''
            GROUP BY e.employee_id
        """)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
            _merge_new_employees(df, parsed_start, parsed_end)
    except Exception as e:
        print(f"Error syncing concierge db: {e}")
    finally:
        engine.dispose()

@router.get("", response_class=HTMLResponse)
async def concierge_tracker_2_page(
    request: Request,
    concierged: str = Query("all"),
    sort: str = Query("employee_id_asc"),
    notice: str = Query(""),
    error: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
    search: str = Query(""),
) -> HTMLResponse:
    _maybe_sync_from_db(start_date, end_date)
    return _render_page(
        request,
        concierged_filter=concierged,
        sort=sort,
        notice=notice,
        error=error,
        start_date=start_date,
        end_date=end_date,
        search=search,
    )


@router.post("/update", response_class=HTMLResponse)
async def update_employee(
    request: Request,
    employee_id: str = Form(...),
    called_date: str = Form(""),
    recruiter: str = Form(""),
    concierged: str | None = Form(None),
    follow_up_status: str = Form(""),
    flag: str = Form(""),
    concierged_filter: str = Form("all"),
    sort: str = Form("employee_id_asc"),
    start_date: str = Form(""),
    end_date: str = Form(""),
    search: str = Form(""),
    notes: str = Form(""),
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

    normalized_status = follow_up_status if follow_up_status in FOLLOW_UP_OPTIONS else ""
    normalized_flag = flag if flag in {"green", "orange", "red"} else ""

    record["called_date"] = normalized_called
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS or recruiter == record.get("recruiter") else ""
    record["concierged"] = concierged is not None
    record["follow_up_status"] = normalized_status
    record["notes"] = notes.strip()
    record["flag"] = normalized_flag

    _save_records(records)

    return _response_for_update(
        request,
        notice="Employee updated.",
        concierged_filter=concierged_filter,
        sort=sort,
        start_date=start_date,
        end_date=end_date,
        search=search,
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
    search: str = "",
) -> HTMLResponse:
    records = _load_records()
    normalized_sort = _normalize_sort(sort)

    effective_start, effective_end = start_date, end_date
    if not start_date and not end_date:
        effective_start, effective_end = _current_week_range()

    parsed_start, parsed_end, date_error = _normalize_filter_dates(effective_start, effective_end)

    filter_error = date_error or ""
    concierged_records = _apply_concierged_filter(records, concierged_filter)
    filtered_records = concierged_records
    concierge_priority_ids: set[str] | None = None
    if not date_error:
        filtered_records = _apply_date_range_filter(concierged_records, parsed_start, parsed_end)
        concierge_priority_ids = _records_with_concierge_in_range(records, parsed_start, parsed_end)

    search_matches = _apply_search_filter(records, search)
    if search_matches:
        filtered_records = _merge_records(filtered_records, search_matches)
        if concierge_priority_ids is not None:
            concierge_priority_ids.update(
                record.get("employee_id")
                for record in search_matches
                if _date_in_range(record.get("concierge_date", ""), parsed_start, parsed_end)
            )

    sorted_records = _apply_sort(filtered_records, normalized_sort, concierge_priority_ids)

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
        "search": search,
        "follow_up_options": FOLLOW_UP_OPTIONS,
    }

    status_code = 400 if combined_error else 200
    return templates.TemplateResponse("apps/concierge_tracker_2.html", context, status_code=status_code)


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
    search: str = "",
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
        search=search,
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

    return [
        record
        for record in records
        if _date_in_range(record.get("start_date", ""), start_date, end_date)
        or _date_in_range(record.get("rehire_date", ""), start_date, end_date)
        or _date_in_range(record.get("concierge_date", ""), start_date, end_date)
    ]


def _apply_sort(
    records: List[Dict[str, object]],
    sort: str,
    concierge_priority_ids: set[str] | None = None,
) -> List[Dict[str, object]]:
    key_func, reverse = _SORT_FIELDS.get(sort, _SORT_FIELDS["employee_id_asc"])
    sorted_records = sorted(records, key=key_func, reverse=reverse)

    if concierge_priority_ids:
        prioritized = sorted_records.copy()
        prioritized.sort(
            key=lambda record: 0
            if record.get("employee_id") in concierge_priority_ids
            else 1
        )
        return prioritized

    return sorted_records


def _normalize_sort(sort: str) -> str:
    normalized = (sort or "").lower()
    if normalized in _SORT_FIELDS:
        return normalized
    return "employee_id_asc"


def _apply_search_filter(
    records: List[Dict[str, object]], search: str
) -> List[Dict[str, object]]:
    if not search:
        return []

    query = search.strip().lower()
    if not query:
        return []

    def _matches(record: Dict[str, object]) -> bool:
        first = str(record.get("first_name", "")).lower()
        last = str(record.get("last_name", "")).lower()
        full_name = f"{first} {last}".strip()
        return query in first or query in last or query in full_name

    return [record for record in records if _matches(record)]


def _merge_records(
    base_records: List[Dict[str, object]],
    additional_records: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    existing_ids = {record.get("employee_id") for record in base_records}
    merged = list(base_records)

    for record in additional_records:
        employee_id = record.get("employee_id")
        if employee_id not in existing_ids:
            merged.append(record)
            existing_ids.add(employee_id)

    return merged


def _date_in_range(
    date_str: str, start_date: pd.Timestamp | None, end_date: pd.Timestamp | None
) -> bool:
    parsed = _normalize_date(date_str)
    if parsed is None:
        return False
    if start_date is not None and parsed < start_date:
        return False
    if end_date is not None and parsed > end_date:
        return False
    return True


def _records_with_concierge_in_range(
    records: List[Dict[str, object]],
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> set[str]:
    if start_date is None and end_date is None:
        return set()

    return {
        str(record.get("employee_id"))
        for record in records
        if _date_in_range(record.get("concierge_date", ""), start_date, end_date)
    }


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


def _merge_new_employees(dataframe: pd.DataFrame, filter_start: pd.Timestamp | None = None, filter_end: pd.Timestamp | None = None) -> int:
    records = _load_records()
    
    if "DB Employee ID" in dataframe.columns:
        db_eids = {str(eid).strip() for eid in dataframe["DB Employee ID"] if str(eid).strip()}
        records = [r for r in records if str(r.get("employee_id")).strip() not in db_eids]
        
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

        in_range = False
        if filter_start is None and filter_end is None:
            in_range = True
        else:
            in_range = (
                _date_in_range(formatted_start, filter_start, filter_end) or
                _date_in_range(formatted_rehire, filter_start, filter_end)
            )

        needs_rehire_tag = False
        if rehire_date is not None:
            last_timesheet = _normalize_date(row.get("Last Timesheet Date"))
            two_years_ago = pd.Timestamp.today().normalize() - pd.DateOffset(years=2)
            
            if last_timesheet is not None:
                if last_timesheet < two_years_ago:
                    needs_rehire_tag = True
            else:
                if start_date is not None and start_date < two_years_ago:
                    needs_rehire_tag = True

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
            if "DB Employee ID" in row:
                record["db_employee_id"] = str(row["DB Employee ID"]).strip()
            
            record["needs_rehire_tag"] = needs_rehire_tag
            continue

        if not in_range:
            continue

        record = {
            "employee_id": employee_id,
            "first_name": str(row.get("First Name", "")).strip(),
            "last_name": str(row.get("Last Name", "")).strip(),
            "start_date": formatted_start,
            "rehire_date": formatted_rehire,
            "concierge_date": formatted_concierge,
            "called_date": "",
            "recruiter": "",
            "concierged": concierge_date is not None,
            "mobile": mobile,
            "language": language,
            "db_employee_id": str(row.get("DB Employee ID", "")).strip(),
            "follow_up_status": "",
            "notes": "",
            "flag": "",
            "needs_rehire_tag": needs_rehire_tag,
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
        "recruiter": "",
        "concierged": False,
        "follow_up_status": "",
        "mobile": "",
        "language": "",
        "db_employee_id": "",
        "notes": "",
        "flag": "",
        "needs_rehire_tag": False,
    }
    for key, value in defaults.items():
        record.setdefault(key, value)
    return record

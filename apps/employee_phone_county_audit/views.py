from __future__ import annotations

from io import BytesIO
import re
import json
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


templates = Jinja2Templates(directory="templates")
router = APIRouter()

REQUIRED_COLUMNS = {
    "Status",
    "Employee ID",
    "Mobile",
    "County of Residence",
    "First Name",
    "Last Name",
    "Email",
}

STATUS_ALLOWED = {"Active", "Inactive (60)"}

OUTPUT_COLUMNS = [
    "Employee ID",
    "First Name",
    "Last Name",
    "Mobile",
    "County of Residence",
    "Email",
]

MOBILE_OUTPUT_COLUMNS = [
    "Employee ID",
    "First Name",
    "Last Name",
    "Mobile",
    "Original Phone",
    "County of Residence",
    "Email",
]

COUNTY_OUTPUT_COLUMNS = [
    "Employee ID",
    "First Name",
    "Last Name",
    "Mobile",
    "County of Residence",
    "Email",
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    return normalized


def _validate_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise HTTPException(
            status_code=400,
            detail=f"Uploaded file is missing required columns: {missing_str}.",
        )


def _valid_employee_id(value) -> bool:
    if pd.isna(value):
        return False

    cleaned = str(value).strip()
    if cleaned == "" or cleaned.lower() in {"nan", "none", "null"}:
        return False

    if "deleted" in cleaned.lower():
        return False

    return re.search(r"[A-Za-z0-9]", cleaned) is not None


def _normalize_phone(value) -> str:
    if pd.isna(value):
        return ""

    cleaned = str(value)
    for char in (" ", "-", "(", ")", ".", "+"):
        cleaned = cleaned.replace(char, "")
    return cleaned


def _get_original_phone(employee_id) -> str:
    """Check the history_entry table to find the correct phone number from Dash Sync notes."""
    try:
        emp_id = int(float(str(employee_id).strip()))
    except (ValueError, TypeError):
        return ""

    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    sql = text("""
        SELECT changes 
        FROM history_entry 
        WHERE related = 'Employee' 
          AND related_id = :employee_id 
          AND notes = 'Dash Sync'
        ORDER BY created_at DESC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"employee_id": emp_id}).fetchall()
            for row in rows:
                changes_str = row[0]
                if not changes_str:
                    continue
                try:
                    changes_json = json.loads(changes_str)
                    if isinstance(changes_json, list):
                        for entry in changes_json:
                            attributes = entry.get("attributes", {})
                            mobile_data = attributes.get("mobile", {})
                            if isinstance(mobile_data, dict):
                                old_phone = mobile_data.get("old")
                                if old_phone:
                                    return str(old_phone)
                    elif isinstance(changes_json, dict):
                        attributes = changes_json.get("attributes", {})
                        mobile_data = attributes.get("mobile", {})
                        if isinstance(mobile_data, dict):
                            old_phone = mobile_data.get("old")
                            if old_phone:
                                return str(old_phone)
                except Exception:
                    pass
    except Exception as e:
        print(f"Error querying history_entry for employee {employee_id}: {e}")
    return ""


def _audit_employee_list(data: bytes) -> Dict[str, pd.DataFrame]:
    try:
        df = pd.read_excel(BytesIO(data))
    except ValueError as exc:  # pragma: no cover - pandas specific error
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file does not contain any data.")

    df = _normalize_columns(df)
    _validate_columns(df)

    filtered = df[df["Status"].astype(str).str.strip().isin(STATUS_ALLOWED)].copy()
    filtered = filtered[filtered["Employee ID"].apply(_valid_employee_id)].copy()

    mobile_normalized = filtered["Mobile"].apply(_normalize_phone)
    mobile_issues = filtered[mobile_normalized.str.startswith("1", na=False)].copy()

    county_issues = filtered[
        filtered["County of Residence"].astype(str).str.contains(
            r"\bAlabama\b", case=False, na=False
        )
    ].copy()

    # Query for original phone number from history_entry changes
    mobile_issues["Original Phone"] = mobile_issues["Employee ID"].apply(_get_original_phone)

    mobile_output = mobile_issues.reindex(columns=MOBILE_OUTPUT_COLUMNS)
    county_output = county_issues.reindex(columns=COUNTY_OUTPUT_COLUMNS)

    return {
        "mobile": mobile_output,
        "county": county_output,
    }


def _dataframe_to_table(df: pd.DataFrame, columns: List[str] = OUTPUT_COLUMNS) -> Dict[str, List[Dict[str, str]]]:
    records = df.to_dict(orient="records")
    rows: List[Dict[str, str]] = []
    for record in records:
        row = {column: record.get(column, "") for column in columns}
        rows.append(row)

    return {
        "columns": columns,
        "rows": rows,
        "count": len(rows),
    }


@router.get("", response_class=HTMLResponse)
async def page(request: Request):
    context = {
        "request": request,
        "audit_error": None,
        "audit_results": None,
        "audit_uploaded_filename": None,
    }
    return templates.TemplateResponse("apps/employee_phone_county_audit.html", context)


@router.post("/process", response_class=HTMLResponse)
async def process(request: Request, file: UploadFile = File(...)):
    context = {
        "request": request,
        "audit_error": None,
        "audit_results": None,
        "audit_uploaded_filename": file.filename,
    }

    allowed_suffixes = (".xlsx", ".xlsm", ".xls")
    if not file.filename.lower().endswith(allowed_suffixes):
        context["audit_error"] = "Please upload an Excel file (e.g., .xlsx)."
        return templates.TemplateResponse(
            "apps/employee_phone_county_audit.html",
            context,
            status_code=400,
        )

    file_bytes = await file.read()
    if not file_bytes:
        context["audit_error"] = "The uploaded file was empty."
        return templates.TemplateResponse(
            "apps/employee_phone_county_audit.html",
            context,
            status_code=400,
        )

    try:
        audit_results = _audit_employee_list(file_bytes)
    except HTTPException as exc:
        context["audit_error"] = exc.detail
        return templates.TemplateResponse(
            "apps/employee_phone_county_audit.html",
            context,
            status_code=exc.status_code,
        )

    context["audit_results"] = {
        "mobile": _dataframe_to_table(audit_results["mobile"], MOBILE_OUTPUT_COLUMNS),
        "county": _dataframe_to_table(audit_results["county"], COUNTY_OUTPUT_COLUMNS),
    }

    return templates.TemplateResponse("apps/employee_phone_county_audit.html", context)

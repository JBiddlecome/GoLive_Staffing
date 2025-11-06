from __future__ import annotations

from io import BytesIO
import re
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

    mobile_output = mobile_issues.reindex(columns=OUTPUT_COLUMNS)
    county_output = county_issues.reindex(columns=OUTPUT_COLUMNS)

    return {
        "mobile": mobile_output,
        "county": county_output,
    }


def _dataframe_to_table(df: pd.DataFrame) -> Dict[str, List[Dict[str, str]]]:
    records = df.to_dict(orient="records")
    rows: List[Dict[str, str]] = []
    for record in records:
        row = {column: record.get(column, "") for column in OUTPUT_COLUMNS}
        rows.append(row)

    return {
        "columns": OUTPUT_COLUMNS,
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
        "mobile": _dataframe_to_table(audit_results["mobile"]),
        "county": _dataframe_to_table(audit_results["county"]),
    }

    return templates.TemplateResponse("apps/employee_phone_county_audit.html", context)

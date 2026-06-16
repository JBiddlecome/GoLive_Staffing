from __future__ import annotations

from io import BytesIO
import re
import json
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel


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


def _normalize_payroll_id(value) -> str:
    """Normalize the employee ID from spreadsheet (which represents payroll_id)
    to a clean string without decimals (e.g. 112492.0 -> '112492').
    """
    if pd.isna(value):
        return ""
    cleaned = str(value).strip()
    if cleaned.endswith(".0"):
        cleaned = cleaned[:-2]
    return cleaned


def _get_employee_id_mapping(payroll_ids: List[str]) -> Dict[str, int]:
    """Map database payroll_id (string) to database employee_id (int)."""
    if not payroll_ids:
        return {}

    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    
    sql = text("""
        SELECT employee_id, payroll_id 
        FROM employee 
        WHERE payroll_id IN :payroll_ids
          AND deleted_at IS NULL
    """)
    
    mapping = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"payroll_ids": tuple(payroll_ids)}).fetchall()
            for row in rows:
                emp_id = row[0]
                pay_id = str(row[1]).strip()
                mapping[pay_id] = emp_id
    except Exception as e:
        print(f"Error querying employee table mapping: {e}")
        
    return mapping


def _get_original_phone(employee_id) -> str:
    """Check the history_entry table to find the correct phone number from notes or changes.
    Accepts either a payroll_id or an employee_id.
    """
    try:
        clean_id = _normalize_payroll_id(employee_id)
        if not clean_id:
            return ""
            
        emp_id = None
        
        # 1. Try to find the employee_id in the database by payroll_id first
        from sqlalchemy import text
        from apps.position_requests.scheduler import _engine
        engine = _engine()
        
        sql_map = text("""
            SELECT employee_id 
            FROM employee 
            WHERE payroll_id = :payroll_id
              AND deleted_at IS NULL
            LIMIT 1
        """)
        
        try:
            with engine.connect() as conn:
                row = conn.execute(sql_map, {"payroll_id": clean_id}).fetchone()
                if row:
                    emp_id = row[0]
        except Exception:
            pass
            
        # 2. Fall back to parsing employee_id directly as integer if not found by payroll_id
        if emp_id is None:
            emp_id = int(float(clean_id))
    except (ValueError, TypeError):
        return ""

    sql = text("""
        SELECT changes 
        FROM history_entry 
        WHERE related = 'Employee' 
          AND related_id = :employee_id 
        ORDER BY created_at DESC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"employee_id": emp_id}).fetchall()
            fallback_phone = ""
            for row in rows:
                changes_str = row[0]
                if not changes_str:
                    continue
                try:
                    changes_json = json.loads(changes_str)
                    entries = changes_json if isinstance(changes_json, list) else [changes_json]
                    for entry in entries:
                        attributes = entry.get("attributes", {})
                        mobile_data = attributes.get("mobile", {})
                        if isinstance(mobile_data, dict):
                            old_phone = mobile_data.get("old")
                            if old_phone:
                                old_phone_str = str(old_phone)
                                normalized = _normalize_phone(old_phone_str)
                                if normalized and not normalized.startswith("1"):
                                    return old_phone_str
                                else:
                                    if not fallback_phone:
                                        fallback_phone = old_phone_str
                except Exception:
                    pass
            if fallback_phone:
                return fallback_phone
    except Exception as e:
        print(f"Error querying history_entry for employee {employee_id}: {e}")
    return ""


def _get_original_phones_bulk(employee_ids: List[int]) -> Dict[int, str]:
    """Check the history_entry table in bulk to find the correct phone numbers."""
    if not employee_ids:
        return {}

    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    
    sql = text("""
        SELECT related_id, changes, notes, created_at
        FROM history_entry 
        WHERE related = 'Employee' 
          AND related_id IN :employee_ids
        ORDER BY created_at DESC
    """)
    
    from collections import defaultdict
    emp_history = defaultdict(list)
    
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"employee_ids": tuple(employee_ids)}).fetchall()
            for row in rows:
                emp_id = row[0]
                changes_str = row[1]
                notes = row[2]
                created_at = row[3]
                emp_history[emp_id].append((changes_str, notes, created_at))
    except Exception as e:
        print(f"Error querying history_entry in bulk for employees {employee_ids}: {e}")
        return {}
        
    resolved_phones = {}
    for emp_id, history in emp_history.items():
        fallback_phone = ""
        for changes_str, notes, created_at in history:
            if not changes_str:
                continue
            try:
                changes_json = json.loads(changes_str)
                entries = changes_json if isinstance(changes_json, list) else [changes_json]
                for entry in entries:
                    attributes = entry.get("attributes", {})
                    mobile_data = attributes.get("mobile", {})
                    if isinstance(mobile_data, dict):
                        old_phone = mobile_data.get("old")
                        if old_phone:
                            old_phone_str = str(old_phone)
                            normalized = _normalize_phone(old_phone_str)
                            if normalized and not normalized.startswith("1"):
                                resolved_phones[emp_id] = old_phone_str
                                break
                            else:
                                if not fallback_phone:
                                    fallback_phone = old_phone_str
                if emp_id in resolved_phones:
                    break
            except Exception:
                pass
        if emp_id not in resolved_phones and fallback_phone:
            resolved_phones[emp_id] = fallback_phone
            
    return resolved_phones


def _audit_employee_list(data: bytes) -> Dict[str, pd.DataFrame]:
    try:
        df = pd.read_excel(BytesIO(data))
    except ValueError as exc:  # pragma: no cover - pandas specific error
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file does not contain any data.")

    df = _normalize_columns(df)
    _validate_columns(df)

    # Clean the "Employee ID" column values in the DataFrame (which are actually payroll_ids)
    # so they are cleanly formatted strings without decimal .0 points
    df["Employee ID"] = df["Employee ID"].apply(_normalize_payroll_id)

    filtered = df[df["Status"].astype(str).str.strip().isin(STATUS_ALLOWED)].copy()
    filtered = filtered[filtered["Employee ID"].apply(_valid_employee_id)].copy()

    mobile_normalized = filtered["Mobile"].apply(_normalize_phone)
    mobile_issues = filtered[mobile_normalized.str.startswith("1", na=False)].copy()

    county_issues = filtered[
        filtered["County of Residence"].astype(str).str.contains(
            r"\bAlabama\b", case=False, na=False
        )
    ].copy()

    # Extract unique payroll IDs from mobile_issues
    payroll_ids = [pid for pid in mobile_issues["Employee ID"].dropna().unique() if pid != ""]

    # Map payroll IDs to database employee IDs in bulk
    payroll_to_emp_map = _get_employee_id_mapping(payroll_ids)
    emp_ids = list(payroll_to_emp_map.values())

    # Query for original phone numbers from history_entry changes in bulk
    original_phones = _get_original_phones_bulk(emp_ids)

    def map_original_phone(payroll_id):
        emp_id = payroll_to_emp_map.get(payroll_id)
        if emp_id:
            return original_phones.get(emp_id, "")
        return ""

    # Set the resolved original phone numbers
    mobile_issues["Original Phone"] = mobile_issues["Employee ID"].apply(map_original_phone)

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


class UpdatePhoneRequest(BaseModel):
    payroll_id: str
    original_phone: str


@router.post("/update-phone")
async def update_phone(request: Request, payload: UpdatePhoneRequest):
    user = request.session.get("user")
    user_id = user.get("id") if user else None
    
    payroll_id = _normalize_payroll_id(payload.payroll_id)
    new_mobile = payload.original_phone.strip()
    
    if not payroll_id or not new_mobile:
        raise HTTPException(status_code=400, detail="Missing payroll_id or original_phone.")
        
    from apps.position_requests.scheduler import _engine
    from sqlalchemy import text
    engine = _engine()
    
    # 1. Map payroll_id to employee_id and fetch current mobile number
    sql_find = text("""
        SELECT employee_id, mobile 
        FROM employee 
        WHERE payroll_id = :payroll_id 
          AND deleted_at IS NULL
        LIMIT 1
    """)
    
    try:
        with engine.begin() as conn:
            row = conn.execute(sql_find, {"payroll_id": payroll_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Active employee with Payroll ID {payroll_id} not found.")
                
            employee_id = row[0]
            current_mobile = row[1]
            
            # 2. Update the mobile field in the employee table
            sql_update = text("""
                UPDATE employee 
                SET mobile = :new_mobile, updated_on = NOW()
                WHERE employee_id = :employee_id
            """)
            conn.execute(sql_update, {"new_mobile": new_mobile, "employee_id": employee_id})
            
            # 3. Log the change in history_entry
            changes = [{
                "model": ["Employee", employee_id],
                "operation": "update",
                "attributes": {
                    "mobile": {
                        "old": current_mobile,
                        "new": new_mobile
                    }
                },
                "description": None
            }]
            
            sql_history = text("""
                INSERT INTO history_entry (related, related_id, model, model_id, changes, notes, created_at, created_by)
                VALUES ('Employee', :related_id, 'Employee', :model_id, :changes, 'Phone Audit Fix', NOW(), :created_by)
            """)
            conn.execute(sql_history, {
                "related_id": employee_id,
                "model_id": employee_id,
                "changes": json.dumps(changes),
                "created_by": user_id
            })
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating phone number for payroll_id {payroll_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
    return {"status": "success", "message": f"Successfully updated mobile number for employee #{payroll_id} to {new_mobile}."}


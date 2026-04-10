from __future__ import annotations

import os
from io import StringIO
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from apps.context import default_employee_filter_context, default_text_blast_context

templates = Jinja2Templates(directory="templates")
router = APIRouter()

EXPECTED_COLUMNS: List[str] = [
    "Shift Position Title",
    "Shift Start",
    "Shift End",
    "Payroll ID",
    "Employee Name",
    "Employee Status",
    "Employee Phone",
    "Email Address",
    "1st Shift",
    "1st Venue",
    "Start Date",
    "Last Shift Worked",
    "Miles from Location",
    "Preferred",
]

CANDIDATE_STATUSES = {
    1: "Active",
    2: "Candidate",
    3: "Hiatus",
    5: "Terminated",
    6: "Resigned",
    10: "Inactive (60)",
    14: "Other Status",
    15: "Ineligible for Rehire"
}

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST", "localhost")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))

    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

UPLOAD_DIR = Path("tmp/text_blast_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _load_dataframe(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, skiprows=3)
    except ValueError as exc:  # pragma: no cover - pandas specific error
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file does not contain any data.")

    if len(df.columns) < len(EXPECTED_COLUMNS):
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is missing required columns after removing the first three rows.",
        )

    df = df.iloc[:, : len(EXPECTED_COLUMNS)].copy()
    df.columns = EXPECTED_COLUMNS
    df = df.dropna(how="all")
    return df


def _collect_filter_options(df: pd.DataFrame) -> Dict[str, List[str]]:
    def _prepare(series: pd.Series) -> List[str]:
        cleaned = series.dropna().astype(str).str.strip()
        return sorted({value for value in cleaned if value})

    return {
        "shift_positions": _prepare(df["Shift Position Title"]),
        "employee_statuses": _prepare(df["Employee Status"]),
    }


@router.get("")
async def page(request: Request):
    context: Dict[str, object] = {"request": request}
    context.update(default_text_blast_context())
    context.update(default_employee_filter_context())

    engine = _engine()
    county_choices = []
    try:
        with engine.connect() as conn:
            rs = conn.execute(text("SELECT id, name FROM county ORDER BY name"))
            for row in rs:
                county_choices.append((row[0], row[1]))
    except Exception:
        pass
    finally:
        engine.dispose()
        
    context.update({
        "candidate_statuses": CANDIDATE_STATUSES,
        "county_choices": county_choices
    })

    return templates.TemplateResponse("apps/text_blast_filter.html", context)


@router.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith(".xlsx"):
        context: Dict[str, object] = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"text_error": "Please upload an .xlsx file exported from the event."})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
            status_code=400,
        )

    file_contents = await file.read()
    if not file_contents:
        context: Dict[str, object] = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"text_error": "The uploaded file was empty."})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
            status_code=400,
        )

    file_suffix = Path(file.filename).suffix or ".xlsx"
    file_token = f"{uuid4().hex}{file_suffix}"
    saved_path = UPLOAD_DIR / file_token
    saved_path.write_bytes(file_contents)

    try:
        dataframe = _load_dataframe(saved_path)
    except HTTPException as exc:
        saved_path.unlink(missing_ok=True)
        context: Dict[str, object] = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"text_error": exc.detail})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
            status_code=exc.status_code,
        )

    options = _collect_filter_options(dataframe)

    context = {"request": request}
    context.update(default_text_blast_context())
    context.update(default_employee_filter_context())
    context.update(
        {
            "text_options": options,
            "text_file_token": file_token,
            "text_uploaded_filename": file.filename,
        }
    )

    return templates.TemplateResponse("apps/text_blast_filter.html", context)


def _apply_filters(
    df: pd.DataFrame,
    shift_position_title: str,
    employee_status: str,
    miles_from_location: float,
) -> pd.DataFrame:
    filtered = df.copy()

    if shift_position_title:
        filtered = filtered[filtered["Shift Position Title"].astype(str).str.strip() == shift_position_title]

    if employee_status:
        filtered = filtered[filtered["Employee Status"].astype(str).str.strip() == employee_status]

    miles_series = pd.to_numeric(filtered["Miles from Location"], errors="coerce")
    filtered = filtered[miles_series <= miles_from_location]

    return filtered


def _clean_phone_numbers(df: pd.DataFrame) -> pd.DataFrame:
    phones = df["Employee Phone"].fillna("").astype(str)
    phones = phones.str.replace(r"\D", "", regex=True)

    valid_mask = phones.str.len() > 0
    valid_mask &= ~phones.str.fullmatch(r"0+")
    valid_mask &= ~phones.str.startswith("1")

    cleaned = df.loc[valid_mask].copy()
    cleaned.loc[:, "Employee Phone"] = phones.loc[valid_mask]
    return cleaned


def _split_employee_name(name: str) -> Tuple[str, str]:
    if not isinstance(name, str):
        return "", ""

    stripped = name.strip()
    if not stripped:
        return "", ""

    if "," in stripped:
        last, first = [part.strip() for part in stripped.split(",", 1)]
        return first, last

    parts = stripped.split()
    if len(parts) == 1:
        return parts[0], ""

    first = parts[0]
    last = " ".join(parts[1:])
    return first, last


@router.post("/process")
async def process(
    file_token: str = Form(...),
    shift_position_title: str = Form(""),
    employee_status: str = Form(""),
    miles_from_location: str = Form("50"),
):
    saved_path = (UPLOAD_DIR / Path(file_token).name).resolve()

    if not str(saved_path).startswith(str(UPLOAD_DIR.resolve())) or not saved_path.exists():
        raise HTTPException(status_code=400, detail="The uploaded file could not be found. Please upload it again.")

    dataframe = _load_dataframe(saved_path)

    try:
        miles_value = float(miles_from_location)
    except (TypeError, ValueError):
        miles_value = 50.0
    miles_value = max(miles_value, 0.0)

    filtered = _apply_filters(dataframe, shift_position_title.strip(), employee_status.strip(), miles_value)
    cleaned = _clean_phone_numbers(filtered)
    cleaned = cleaned.drop_duplicates(subset=["Payroll ID"])

    cleaned = cleaned.copy()
    if cleaned.empty:
        cleaned["First Name"] = []
        cleaned["Last Name"] = []
    else:
        first_names, last_names = zip(*cleaned["Employee Name"].map(_split_employee_name))
        cleaned.loc[:, "First Name"] = list(first_names)
        cleaned.loc[:, "Last Name"] = list(last_names)

    output_columns = [
        "Payroll ID",
        "First Name",
        "Last Name",
        "Employee Name",
        "Shift Position Title",
        "Shift Start",
        "Shift End",
        "Employee Status",
        "Employee Phone",
        "Email Address",
        "1st Shift",
        "1st Venue",
        "Start Date",
        "Last Shift Worked",
        "Miles from Location",
        "Preferred",
    ]

    available_columns = [column for column in output_columns if column in cleaned.columns]
    cleaned = cleaned.loc[:, available_columns]

    output_buffer = StringIO()
    cleaned.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    filename = f"sms_list_{uuid4().hex}.csv"
    return StreamingResponse(
        output_buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

@router.post("/candidate-report")
async def candidate_report(
    candidate_status: str = Form(""),
    counties: List[str] = Form(default=[]),
    created_start: str = Form(""),
    created_end: str = Form("")
):
    engine = _engine()
    try:
        query = """
            SELECT e.status, e.first_name, e.last_name, e.mobile, e.created_on, e.email, c.name as county
            FROM employee e
            LEFT JOIN county c ON e.county_id = c.id
            WHERE 1=1
        """
        params = {}
        
        if candidate_status:
            query += " AND e.status = :status"
            params["status"] = int(candidate_status)
            
        if counties:
            valid_counties = [int(x) for x in counties if x.isdigit()]
            if valid_counties:
                placeholders = ', '.join([f':c{i}' for i in range(len(valid_counties))])
                query += f" AND e.county_id IN ({placeholders})"
                for i, cid in enumerate(valid_counties):
                    params[f"c{i}"] = cid
                    
        if created_start:
            query += " AND DATE(e.created_on) >= :created_start"
            params["created_start"] = created_start
            
        if created_end:
            query += " AND DATE(e.created_on) <= :created_end"
            params["created_end"] = created_end
            
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
            
        df['Status'] = df['status'].map(CANDIDATE_STATUSES).fillna("Unknown Status")
        df['First Name'] = df['first_name']
        df['Last Name'] = df['last_name']
        
        def format_mobile(mobile):
            if pd.isna(mobile):
                return ""
            digits = ''.join(filter(str.isdigit, str(mobile)))
            if not digits:
                return ""
            if len(digits) == 10:
                return '1' + digits
            elif len(digits) > 10 and digits.startswith('1'):
                return digits
            return '1' + digits

        df['Mobile'] = df['mobile'].apply(format_mobile)
        df['Email'] = df['email']
        df['County'] = df['county']
        
        output_columns = ["Status", "First Name", "Last Name", "Mobile", "Email", "County"]
        df_out = df[output_columns]
        
        output_buffer = StringIO()
        df_out.to_csv(output_buffer, index=False)
        output_buffer.seek(0)

        filename = f"candidate_contact_report_{uuid4().hex[:8]}.csv"
        return StreamingResponse(
            output_buffer,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
            
    finally:
        engine.dispose()

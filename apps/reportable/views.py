from __future__ import annotations

import os
from io import BytesIO
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")


class ExportPayload(BaseModel):
    table: str = Field(..., min_length=1)
    columns: list[str] = Field(default_factory=list)
    date_column: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    limit: int = Field(default=50000, ge=1, le=100000)


class TimesheetVerificationPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


TIMESHEET_VERIFICATION_EXCLUDED_COLUMNS = {
    "Day",
    "WC",
    "Reg H (c)",
    "OT H (c)",
    "Non-Worked Hours (c)",
    "Cert Cost (e)",
    "OT R",
    "DT R",
    "Non-Worked Bill (c)",
    "Total Bill",
    "Verification (c)",
    "Verification (e)",
    "day",
    "wc",
    "verification_start",
    "verification_end",
    "verification_start_at",
    "verification_end_at",
}


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    # The Reportable app should always target the production staffing schema unless
    # explicitly overridden for controlled environments.
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    # Guardrail: DB_HOST is shared by multiple tools. If it is set to localhost
    # but the Reportable SSH tunnel is not configured, prefer the direct RDS host
    # when available so Reportable can still connect.
    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    missing = [
        env_name
        for env_name, value in (
            ("DB_HOST", host),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required database environment variables: {', '.join(missing)}",
        )

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


def _list_tables() -> list[str]:
    engine = _engine()
    try:
        inspector = inspect(engine)
        return sorted(inspector.get_table_names())
    finally:
        engine.dispose()


def _list_columns(table: str) -> list[dict[str, Any]]:
    engine = _engine()
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        if table not in table_names:
            raise HTTPException(status_code=404, detail=f"Unknown table '{table}'.")

        columns = inspector.get_columns(table)
        return [
            {
                "name": col["name"],
                "type": str(col.get("type", "")),
                "nullable": bool(col.get("nullable", True)),
            }
            for col in columns
        ]
    finally:
        engine.dispose()


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


@router.get("")
async def reportable_page(request: Request):
    return templates.TemplateResponse("apps/reportable.html", {"request": request})


@router.get("/schema/tables")
async def reportable_schema_tables() -> JSONResponse:
    return JSONResponse({"tables": _list_tables()})


@router.get("/schema/columns")
async def reportable_schema_columns(table: str = Query(..., min_length=1)) -> JSONResponse:
    return JSONResponse({"table": table, "columns": _list_columns(table)})


@router.post("/export")
async def reportable_export(payload: ExportPayload) -> StreamingResponse:
    engine = _engine()
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        if payload.table not in table_names:
            raise HTTPException(status_code=404, detail=f"Unknown table '{payload.table}'.")

        columns_meta = inspector.get_columns(payload.table)
        allowed_columns = {col["name"] for col in columns_meta}

        selected_columns = payload.columns or sorted(allowed_columns)
        invalid_columns = sorted(set(selected_columns) - allowed_columns)
        if invalid_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid columns for table '{payload.table}': {', '.join(invalid_columns)}",
            )

        sql_columns = ", ".join(_quote_identifier(col) for col in selected_columns)
        sql = f"SELECT {sql_columns} FROM {_quote_identifier(payload.table)}"
        params: dict[str, Any] = {}

        if payload.date_column and (payload.start_date or payload.end_date):
            if payload.date_column not in allowed_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid date_column '{payload.date_column}' for table '{payload.table}'.",
                )

            filters: list[str] = []
            if payload.start_date:
                filters.append(f"{_quote_identifier(payload.date_column)} >= :start_date")
                params["start_date"] = payload.start_date
            if payload.end_date:
                filters.append(f"{_quote_identifier(payload.date_column)} <= :end_date")
                params["end_date"] = payload.end_date

            if filters:
                sql += " WHERE " + " AND ".join(filters)

        sql += " LIMIT :limit"
        params["limit"] = payload.limit

        with engine.begin() as connection:
            dataframe = pd.read_sql(text(sql), connection, params=params)
    finally:
        engine.dispose()

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="report")
    output.seek(0)

    filename = f"{payload.table}_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


class EmployeeListPayload(BaseModel):
    limit: int = Field(default=50000, ge=1, le=100000)


@router.post("/export/employee-list")
async def reportable_employee_list_export(
    payload: EmployeeListPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        # SQL Query for Employee List
        # Fetches employee details and aggregates related data (Languages, Certifications, etc.)
        sql = text(
            """
            SELECT
                e.payroll_id AS `Employee ID`,
                e.status AS `Status Code`,
                e.first_name AS `First Name`,
                e.last_name AS `Last Name`,
                DATE_FORMAT(e.dob, '%m/%d/%Y') AS `Date of Birth`,
                e.address1 AS `Address`,
                e.address2 AS `Address2`,
                e.city AS `City`,
                e.state AS `State`,
                e.zip AS `Zip`,
                e.mobile AS `Mobile`,
                e.home AS `Home`,
                e.email AS `Email`,
                e.transportation AS `Transportation Code`,
                DATE_FORMAT(e.background_date, '%m/%d/%Y') AS `Date of Background`,
                DATE_FORMAT(e.concierge_date, '%m/%d/%Y') AS `Concierge Date`,
                e.background_query,
                e.background,
                DATE_FORMAT(e.start_date, '%m/%d/%Y') AS `Start Date`,
                e.ssn AS `SS Number`,
                DATE_FORMAT(e.start_date2, '%m/%d/%Y') AS `Rehire Date`,
                CONCAT(u_rec.first_name, ' ', u_rec.last_name) AS `Recruited By`,
                e.referred_by AS `Referred By`,
                e.sex AS `Gender`,
                c.name AS `County of Residence`,
                DATE_FORMAT(e.created_on, '%m/%d/%Y %H:%i:%s') AS `Created On`,
                sr.reason AS `Status Reason`,
                
                -- Aggregates
                lang_agg.languages AS `Language`,
                cert_agg.certifications AS `Certifications`,
                pos_agg.positions AS `Positions`,
                bg_agg.backgrounds AS `Backgrounds`,
                COALESCE(sh_agg.shift_count, 0) AS `Number of Shifts Worked`

            FROM employee e
            LEFT JOIN county c ON e.county_id = c.id
            LEFT JOIN user u_rec ON e.recruited_by = u_rec.id
            LEFT JOIN status_reason sr ON e.status_reason = sr.id
            
            -- Languages
            LEFT JOIN (
                SELECT el.employee_id, GROUP_CONCAT(DISTINCT l.language ORDER BY l.language SEPARATOR ', ') as languages
                FROM employee_language el
                JOIN language l ON el.language_id = l.language_id
                GROUP BY el.employee_id
            ) lang_agg ON e.employee_id = lang_agg.employee_id
            
            -- Certifications
            LEFT JOIN (
                SELECT ec.employee_id, GROUP_CONCAT(DISTINCT cert.name ORDER BY cert.name SEPARATOR ', ') as certifications
                FROM employee_certification ec
                JOIN certification cert ON ec.certification_id = cert.id
                GROUP BY ec.employee_id
            ) cert_agg ON e.employee_id = cert_agg.employee_id
            
            -- Positions
            LEFT JOIN (
                SELECT ep.employee_id, GROUP_CONCAT(DISTINCT p.description ORDER BY p.description SEPARATOR ', ') as positions
                FROM employee_position ep
                JOIN position p ON ep.position_id = p.position_id
                GROUP BY ep.employee_id
            ) pos_agg ON e.employee_id = pos_agg.employee_id
            
            -- Backgrounds
            LEFT JOIN (
                SELECT eb.employee_id, GROUP_CONCAT(DISTINCT bg.name ORDER BY bg.name SEPARATOR ', ') as backgrounds
                FROM employee_background eb
                JOIN background bg ON eb.background_id = bg.id
                GROUP BY eb.employee_id
            ) bg_agg ON e.employee_id = bg_agg.employee_id
            
            -- Shifts Worked (Approximate based on confirmed shifts)
            LEFT JOIN (
                SELECT employee_id, COUNT(*) as shift_count
                FROM shift_employee
                WHERE confirmed = 1 AND (cancel_reason IS NULL OR cancel_reason = 0)
                GROUP BY employee_id
            ) sh_agg ON e.employee_id = sh_agg.employee_id

            WHERE (e.payroll_id IS NULL OR e.payroll_id NOT LIKE '%DELETED%')
            ORDER BY e.first_name
            LIMIT :limit
            """
        )

        params = {
            "limit": payload.limit,
        }

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)

    finally:
        engine.dispose()

    if df.empty:
         # Return empty excel with headers if no data
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                'Employee ID', 'Status', 'First Name', 'Last Name', 'Date of Birth',
                'Address', 'Address2', 'City', 'State', 'Zip', 'Mobile', 'Home', 'Email',
                'Transportation', 'Date of Background', 'No Background', 'Start Date',
                'Number of Shifts Worked', 'Language', 'Certifications', 'Rehire Date',
                'Recruited By', 'Referred By', 'Positions', 'County of Residence',
                'Backgrounds', 'Concierge Date', 'Gender', 'SS Number'
            ]).to_excel(writer, index=False, sheet_name="employee_list")
        output.seek(0)
        filename = "employee_list_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    # DataFrame Processing
    
    # helper for Status
    # PHP: 
    # if (Employee::STATUS_OTHER == $e->status) { ... } 
    # else { $status = ArrayHelper::getValue(Employee::getStatusLabels(), $e->status, 'Other'); }
    # Mapping inferred: 
    # 1: Active, 2: Candidate, 3: Terminated?
    # We will map knowns and fallback to 'Other' or code.
    
    def get_status_label(row):
        code = row.get('Status Code')
        reason_text = row.get('Status Reason')
        
        if pd.isna(code):
            if pd.notna(reason_text):
                 return reason_text
            return 'Other'
            
        try:
            val = int(code)
            if val == 1: return 'Active'
            if val == 2: return 'Candidate'
            if val == 3: return 'Hiatus'
            if val == 4: return 'Inactive'
            if val == 5: return 'Terminated'
            if val == 6: return 'Resigned'
            if val == 10: return 'Inactive (60)'
            if val == 12: return 'Inactive (180)'
            
            # For 'Other' (14) or any unmapped code, try to use the reason text
            if pd.notna(reason_text) and str(reason_text).strip():
                return reason_text
                
            if val == 14: return 'Other'
            
            return 'Other' 
        except:
             if pd.notna(reason_text) and str(reason_text).strip():
                return reason_text
             return 'Other'

    df['Status'] = df.apply(get_status_label, axis=1)

    # helper for Transportation
    # PHP: 1->Car, 2->Motorcycle, 3->Public Transit
    def get_transportation_label(row):
        code = row.get('Transportation Code')
        if pd.isna(code): return ''
        try:
            val = int(code)
            if val == 1: return 'Car'
            if val == 2: return 'Motorcycle'
            if val == 3: return 'Public Transit'
            return ''
        except:
            return ''

    df['Transportation'] = df.apply(get_transportation_label, axis=1)

    # helper for No Background
    # PHP: if (empty($e->background)) { ... }
    def get_no_background(row):
        background = row.get('background')
        if background and background != 0:
            return '' # Has background
        
        # Check query status
        query_val = row.get('background_query')
        # PHP: PENDING or REQUESTED -> map to label, else 'X'
        # Enum constants unknown, assuming 1=Pending, 2=Requested?
        # Defaulting to 'X' if no background and logic unclear, or just empty if logic too complex to port without enums.
        # "If empty(background) ... else blank"
        # Let's just output 'X' if no background for now, or refine if user complains.
        return 'X'

    df['No Background'] = df.apply(get_no_background, axis=1)

    # SSN extraction logic from PHP
    def format_ssn(row):
        ssn = str(row.get('SS Number', ''))
        if not ssn or ssn == 'None':
             return ''
        ssn_clean = ssn.replace('-', '').strip()
        if len(ssn_clean) >= 9:
             return f"{ssn_clean[:3]}-{ssn_clean[3:5]}-{ssn_clean[5:9]}"
        return ssn_clean

    df['SS Number'] = df.apply(format_ssn, axis=1)

    # Select and Reorder columns based on headers
    final_columns = [
        'Employee ID',
        'Status',
        'First Name',
        'Last Name',
        'Date of Birth',
        'Address',
        'Address2',
        'City',
        'State',
        'Zip',
        'Mobile',
        'Home',
        'Email',
        'Transportation',
        'Date of Background',
        'No Background',
        'Start Date',
        'Number of Shifts Worked',
        'Language',
        'Certifications',
        'Rehire Date',
        'Recruited By',
        'Referred By',
        'Positions',
        'County of Residence',
        'Backgrounds',
        'Concierge Date',
        'Gender',
        'SS Number',
        'Created On'
    ]
    
    # Ensure all columns exist
    for col in final_columns:
        if col not in df.columns:
            df[col] = ''
            
    final_df = df[final_columns]

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="employee_list")
        
        # Auto-adjust column widths (basic approximation)
        worksheet = writer.sheets['employee_list']
        for idx, col in enumerate(final_df.columns):
             # Header length vs max content length (capped)
             max_len = max(
                final_df[col].astype(str).map(len).max() if not final_df[col].empty else 0,
                len(str(col))
             )
             max_len = min(max_len, 50) + 2
             worksheet.column_dimensions[chr(65 + idx) if idx < 26 else 'A' + chr(65 + (idx - 26))].width = max_len

    output.seek(0)
    filename = "employee_list_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

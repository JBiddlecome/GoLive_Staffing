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
    host = os.getenv("DB_HOST")
    # The Reportable app should always target the production staffing schema unless
    # explicitly overridden for controlled environments.
    name = os.getenv("REPORTABLE_DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

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


@router.post("/export/timesheet-verification")
async def reportable_timesheet_verification_export(
    payload: TimesheetVerificationPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        # 1. Advanced SQL Query matching PHP logic
        sql = text(
            """
            SELECT
                se.shift_employee_id,
                DAYNAME(e.date) AS day_name,
                e.date AS event_date,
                e.state AS event_state,
                COALESCE(wc.wc_code, '8810') AS wc_code,
                c.name AS client_name,
                c.markup AS client_markup,
                v.name AS venue_name,
                e.title AS event_title,
                p.description AS position_name,
                sp.code AS shift_code,
                emp.payroll_id AS emp_number,
                emp.first_name,
                emp.last_name,
                mw.description AS work_state,
                
                -- Rates
                se.bill_rate,
                t.client_service_charge AS service_c,
                t.client_no_break_penalty AS meal_c,
                se.rate AS pay_rate,
                
                -- Timesheet Data
                t.client_start, t.client_end, 
                t.client_break_start, t.client_break_end,
                t.client_seconds, t.employee_seconds,
                TIMESTAMPDIFF(SECOND, s.start, s.end) AS scheduled_seconds,
                
                -- Flags
                t.client_min_bill, t.employee_min_pay,
                t.client_no_bill, t.employee_no_pay,
                t.client_no_break_penalty, t.employee_no_break_penalty,
                
                -- Extras
                t.client_tips, t.client_parking, t.client_travel, t.client_service_charge,
                v.service_charge AS venue_service_charge,
                
                -- Status
                t.start_verified_at, t.end_verified_at,
                t.start_verified, t.end_verified,
                se.confirmed,
                se.cancel_reason
                
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            LEFT JOIN min_wage_rate mw ON emp.min_wage_id = mw.min_wage_id
            LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN shift s ON sp.shift_id = s.shift_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            
            WHERE e.date >= :start_date AND e.date <= :end_date
            AND (
                (se.confirmed = 1 AND (se.cancel_reason IS NULL OR se.cancel_reason = 0))
                OR t.client_min_bill = 1
                OR t.employee_min_pay = 1
            )
            ORDER BY e.date, c.name, emp.last_name, emp.first_name
            LIMIT :limit
            """
        )

        params = {
            "start_date": payload.start_date,
            "end_date": payload.end_date,
            "limit": payload.limit,
        }

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)

    finally:
        engine.dispose()

    # 2. DataFrame Processing
    if df.empty:
        # Return empty excel with headers
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                'Day', 'Date', 'WC', 'Client', 'Markup', 'Venue', 'Event', 'Position', 'Code',
                '#Emp', 'First Name', 'Last Name', 'Work State',
                'Reg H (c)', 'OT H (c)', 'DT H (c)', 'Reg Rate (c)', 'Non-Worked Hours (c)',
                'Cert Cost (e)', 'OT R', 'DT R', 'Tip (c)', 'Park (c)', 'Travel (c)',
                'Service (c)', 'Meal (c)', 'Non-Worked Bill (c)', 'Reimb Pay (e)',
                'Pay Rate', 'Bill Rate', 'Total Bill', 'Status', 'Cancellation Reason',
                'Verification (c)', 'Verification (e)'
            ]).to_excel(writer, index=False, sheet_name="timesheet_verification")
        output.seek(0)
        headers = {"Content-Disposition": 'attachment; filename="timesheet_verification_report.xlsx"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    df.fillna({
        'client_seconds': 0, 
        'bill_rate': 0,
        'client_tips': 0, 'client_parking': 0, 'client_travel': 0, 'client_service_charge': 0,
        'venue_service_charge': 0, 'client_no_break_penalty': 0
    }, inplace=True)

    def calculate_row(row):
        # --- Hours Calculation ---
        # Logic: If client_min_bill is set, ensure at least 4 hours (or split scheduled)
        # Simplified for Python port: Use seconds / 3600
        hours = row['client_seconds'] / 3600.0
        
        if row['client_min_bill'] == 1 and hours < 4.0:
             # Basic min bill rule: usually 4 hours
             hours = max(hours, 4.0)

        # Apply No Bill flag
        if row['client_no_bill'] == 1:
            hours = 0.0

        # --- Overtime Logic (Client Side) ---
        reg_h = hours
        ot_h = 0.0
        dt_h = 0.0

        state = str(row['event_state']).upper() if row['event_state'] else ""
        
        if state in ['CA', 'CALIFORNIA']:
            # CA: >8 OT, >12 DT
            if hours > 12:
                dt_h = hours - 12
                ot_h = 4
                reg_h = 8
            elif hours > 8:
                ot_h = hours - 8
                reg_h = 8
            else:
                reg_h = hours
        elif state in ['NV', 'NEVADA']:
             # NV: >8 OT (simplified, typically depends on 24h period but widely applied as >8 daily)
            if hours > 8:
                ot_h = hours - 8
                reg_h = 8
            else:
                reg_h = hours

        # --- Rates ---
        bill_rate = float(row['bill_rate'])
        ot_rate = bill_rate * 1.5
        dt_rate = bill_rate * 2.0

        # --- Amounts ---
        reg_bill = reg_h * bill_rate
        ot_bill = ot_h * ot_rate
        dt_bill = dt_h * dt_rate

        # --- Meal Penalty ---
        # If penalty exists, charge 1 hour at bill rate
        meal_penalty_bill = 0.0
        if row['client_no_break_penalty'] > 0:
            meal_penalty_bill = bill_rate * 1.0

        # --- Service Charge ---
        # Percentage of billable amount + Flat venue charge
        service_pct = float(row['client_service_charge'] or 0)
        venue_flat = float(row['venue_service_charge'] or 0)
        
        # Service charge usually applies to Total Billable Wages (Reg+OT+DT)
        wages_sum = reg_bill + ot_bill + dt_bill
        service_amt = ((wages_sum * service_pct) / 100.0) + venue_flat

        # --- Extras ---
        tips = float(row['client_tips'])
        parking = float(row['client_parking'])
        travel = float(row['client_travel'])

        # --- Total Bill ---
        total_bill = reg_bill + ot_bill + dt_bill + meal_penalty_bill + service_amt + tips + parking + travel

        # --- Verification Status ---
        ver_c = "Verified" if row['start_verified_at'] else "Pending"
        ver_e = "Verified" if row['end_verified_at'] else "Pending"

        return pd.Series({
            "Reg H (c)": reg_h,
            "OT H (c)": ot_h,
            "DT H (c)": dt_h,
            "Reg Rate (c)": bill_rate,
            "OT R": ot_rate,
            "DT R": dt_rate,
            "Service (c)": service_amt,
            "Meal (c)": meal_penalty_bill,
            "Total Bill": total_bill,
            "Verification (c)": ver_c,
            "Verification (e)": ver_e
        })

    # Apply calculations
    calculated_data = df.apply(calculate_row, axis=1)
    df = pd.concat([df, calculated_data], axis=1)

    # 3. Final Formatting
    final_df = pd.DataFrame()
    final_df['Day'] = df['day_name']
    final_df['Date'] = pd.to_datetime(df['event_date']).dt.date
    final_df['WC'] = df['wc_code']
    final_df['Client'] = df['client_name']
    final_df['Markup'] = df['client_markup']
    final_df['Venue'] = df['venue_name']
    final_df['Event'] = df['event_title']
    final_df['Position'] = df['position_name']
    final_df['Code'] = df['shift_code']
    final_df['#Emp'] = df['emp_number']
    final_df['First Name'] = df['first_name']
    final_df['Last Name'] = df['last_name']
    final_df['Work State'] = df['work_state']
    
    final_df['Reg H (c)'] = df['Reg H (c)'].round(2)
    final_df['OT H (c)'] = df['OT H (c)'].round(2)
    final_df['DT H (c)'] = df['DT H (c)'].round(2)
    final_df['Reg Rate (c)'] = df['Reg Rate (c)'].round(2)
    final_df['Non-Worked Hours (c)'] = 0.0 # Placeholder
    
    final_df['Cert Cost (e)'] = 0.0 # Placeholder
    
    final_df['OT R'] = df['OT R'].round(2)
    final_df['DT R'] = df['DT R'].round(2)
    
    final_df['Tip (c)'] = df['client_tips']
    final_df['Park (c)'] = df['client_parking']
    final_df['Travel (c)'] = df['client_travel']
    final_df['Service (c)'] = df['Service (c)'].round(2)
    final_df['Meal (c)'] = df['Meal (c)'].round(2)
    final_df['Non-Worked Bill (c)'] = 0.0
    final_df['Reimb Pay (e)'] = 0.0
    
    final_df['Pay Rate'] = df['pay_rate']
    final_df['Bill Rate'] = df['bill_rate']
    final_df['Total Bill'] = df['Total Bill'].round(2)
    
    # Status Logic
    def get_status(row):
        if row['cancel_reason'] and row['cancel_reason'] > 0:
            return "CANCELLED"
        return "WORKED" # Simplified
    
    final_df['Status'] = df.apply(get_status, axis=1)
    final_df['Cancellation Reason'] = df['cancel_reason']
    
    final_df['Verification (c)'] = df['Verification (c)']
    final_df['Verification (e)'] = df['Verification (e)']

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="timesheet_verification")
    output.seek(0)

    filename = "timesheet_verification_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

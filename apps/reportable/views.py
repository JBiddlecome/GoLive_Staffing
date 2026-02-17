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


def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

    missing = [
        env_name
        for env_name, value in (
            ("DB_HOST", host),
            ("DB_NAME", name),
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
        inspector = inspect(engine)
        client_columns = {column["name"] for column in inspector.get_columns("client")}
        shift_position_columns = {column["name"] for column in inspector.get_columns("shift_position")}
        timesheet_columns = {column["name"] for column in inspector.get_columns("timesheet")}

        markup_select = "c.markup AS markup" if "markup" in client_columns else "NULL AS markup"
        code_select = "sp.code AS code" if "code" in shift_position_columns else "NULL AS code"
        
        # Additional fields needed for calculation
        # Check if columns exist in timesheet to avoid errors if schema differs slightly
        ts_cols = [
            "client_seconds", "employee_seconds", 
            "client_min_bill", "employee_min_pay", 
            "client_no_bill", "employee_no_pay",
            "client_no_break_penalty", "employee_no_break_penalty"
        ]
        ts_selects = []
        for col in ts_cols:
            if col in timesheet_columns:
                ts_selects.append(f"t.{col}")
            else:
                ts_selects.append(f"0 AS {col}")
        
        ts_select_str = ", ".join(ts_selects)

        sql = text(
            f"""
            SELECT
                DAYNAME(e.date) AS day,
                e.date AS date,
                e.state AS event_state,
                COALESCE(wc.wc_code, CONCAT(emp.state, '8810')) AS wc,
                c.name AS client,
                {markup_select},
                v.name AS venue,
                e.title AS event,
                p.description AS position,
                {code_select},
                emp.payroll_id AS emp_number,
                emp.first_name AS first_name,
                emp.last_name AS last_name,
                mw.description AS work_state,
                se.bill_rate AS reg_rate_c,
                se.rate AS shift_pay_rate,
                t.client_tips AS tip_c,
                t.client_parking AS park_c,
                t.client_travel AS travel_c,
                t.client_service_charge AS service_c,
                t.mealpenalty AS meal_c,
                se.bill_rate AS bill_rate,
                t.client_worked AS status,
                se.cancel_reason AS cancellation_reason,
                t.start_verified AS verification_start,
                t.end_verified AS verification_end,
                t.start_verified_at AS verification_start_at,
                t.end_verified_at AS verification_end_at,
                sp.shift_id,
                v.service_charge AS venue_service_charge,
                {ts_select_str}
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            LEFT JOIN min_wage_rate mw ON emp.min_wage_id = mw.min_wage_id
            LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
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
            
            # Additional fetch for shift durations if needed for min billing calculation
            # We need shift start/end to calculate scheduled duration
            # Since we can't easily join efficiently without row duplication risks or complex logic,
            # we'll fetch shifts separately or do a quick join.
            # Let's try to get shift duration in the main query if possible.
            # Shift table has start/end.
            # Let's simple fetch shift info
            if not df.empty:
                shift_ids = df['shift_id'].unique().tolist()
                if shift_ids:
                    shift_query = text("SELECT shift_id, start, end FROM shift WHERE shift_id IN :shift_ids")
                    shift_df = pd.read_sql(shift_query, connection, params={"shift_ids": tuple(shift_ids)})
                    shift_df['start'] = pd.to_datetime(shift_df['start'])
                    shift_df['end'] = pd.to_datetime(shift_df['end'])
                    shift_df['scheduled_seconds'] = (shift_df['end'] - shift_df['start']).dt.total_seconds()
                    
                    df = df.merge(shift_df[['shift_id', 'scheduled_seconds']], on='shift_id', how='left')
            
    finally:
        engine.dispose()

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
                'Bill Rate', 'Total Bill', 'Status', 'Cancellation Reason',
                'Verification (c)', 'Verification (e)'
            ]).to_excel(writer, index=False, sheet_name="timesheet_verification")
        output.seek(0)
        headers = {"Content-Disposition": 'attachment; filename="timesheet_verification_report.xlsx"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    # --- Calculations ---

    # Fill NaNs
    df.fillna(0, inplace=True)
    
    def calculate_row(row):
        # 1. Calculate Base Hours (Client Side)
        if row['client_min_bill'] and row['scheduled_seconds'] > 0:
            # CA logic: half of scheduled, max 4, min 2
            # Assuming logic applies generally if min_bill is checked, or specifically for Sentinel logic
            # For this report, we'll implement the "Work Hours / 2" logic commonly used
            val = (row['scheduled_seconds'] / 3600) / 2
            client_hours = max(2.0, min(4.0, val))
        else:
            client_hours = row['client_seconds'] / 3600

        if row['client_no_bill']:
            client_hours = 0
            
        # 2. Split into Reg, OT, DT
        # Default logic
        reg_h = client_hours
        ot_h = 0.0
        dt_h = 0.0
        
        state = str(row['event_state']).upper() if row['event_state'] else ""
        
        if state in ['CA', 'NV', 'CALIFORNIA', 'NEVADA']:
             # Daily OT rules
             # CA: >8 OT, >12 DT
             # NV: >8 OT
             
             if state in ['CA', 'CALIFORNIA']:
                 if client_hours > 12:
                     dt_h = client_hours - 12
                     ot_h = 4 # (12-8)
                     reg_h = 8
                 elif client_hours > 8:
                     ot_h = client_hours - 8
                     reg_h = 8
                 else:
                     reg_h = client_hours
             else: # NV
                 if client_hours > 8:
                     ot_h = client_hours - 8
                     reg_h = 8
                 else:
                     reg_h = client_hours
        
        # 3. Rates
        bill_rate = float(row['bill_rate'])
        ot_rate = bill_rate * 1.5
        dt_rate = bill_rate * 2.0
        
        # 4. Amounts
        reg_bill = reg_h * bill_rate
        ot_bill = ot_h * ot_rate
        dt_bill = dt_h * dt_rate
        
        # Service Charge
        # client_service_charge is a percentage in timesheet usually? 
        # In PHP: $client_service_amount = (($bill_rate * $client_service_charge) / 100) + ($venue_service_charge);
        # row['service_c'] comes from t.client_service_charge
        service_pct = float(row['service_c'])
        venue_service_flat = float(row['venue_service_charge'])
        service_amt = ((bill_rate * service_pct) / 100) + venue_service_flat
        
        # Meal Penalty
        # 1 hour at bill rate per penalty?
        # PHP: $client_meal_penalty_amount = $penaltyCalculator->getBillAmount(); which usually is 1 hour pay.
        # We'll assume 1 hour bill rate if penalty exists > 0
        meal_penalty_amt = 0.0
        if row['client_no_break_penalty'] > 0:
             meal_penalty_amt = bill_rate * 1.0 # Simplified assumption
        
        tips = float(row['tip_c'])
        parking = float(row['park_c'])
        travel = float(row['travel_c'])
        
        grand_total = reg_bill + ot_bill + dt_bill + tips + parking + travel + service_amt + meal_penalty_amt
        
        return pd.Series({
            'Reg H (c)': reg_h,
            'OT H (c)': ot_h,
            'DT H (c)': dt_h,
            'Reg Rate (c)': bill_rate,
            'Non-Worked Hours (c)': 0.0, # Placeholder
            'OT R': ot_rate,
            'DT R': dt_rate,
            'Total Bill': grand_total,
            'Service (c)': service_amt,
            'Meal (c)': meal_penalty_amt, # Using Meal (c) column for penalty amount or just cost? Header says Meal (c).
        })

    calc_df = df.apply(calculate_row, axis=1)
    df = pd.concat([df, calc_df], axis=1)

    # Map to final columns
    final_df = pd.DataFrame()
    final_df['Day'] = df['day']
    final_df['Date'] = df['date']
    final_df['WC'] = df['wc']
    final_df['Client'] = df['client']
    final_df['Markup'] = df['markup']
    final_df['Venue'] = df['venue']
    final_df['Event'] = df['event']
    final_df['Position'] = df['position']
    final_df['Code'] = df['code']
    final_df['#Emp'] = df['emp_number']
    final_df['First Name'] = df['first_name']
    final_df['Last Name'] = df['last_name']
    final_df['Work State'] = df['work_state']
    
    final_df['Reg H (c)'] = df['Reg H (c)'].round(2)
    final_df['OT H (c)'] = df['OT H (c)'].round(2)
    final_df['DT H (c)'] = df['DT H (c)'].round(2)
    final_df['Reg Rate (c)'] = df['Reg Rate (c)'].round(2)
    final_df['Non-Worked Hours (c)'] = df['Non-Worked Hours (c)']
    
    final_df['Cert Cost (e)'] = 0.0 # Placeholder
    
    final_df['OT R'] = df['OT R'].round(2)
    final_df['DT R'] = df['DT R'].round(2)
    
    final_df['Tip (c)'] = df['tip_c']
    final_df['Park (c)'] = df['park_c']
    final_df['Travel (c)'] = df['travel_c']
    final_df['Service (c)'] = df['Service (c)'].round(2)
    final_df['Meal (c)'] = df['Meal (c)'].round(2)
    final_df['Non-Worked Bill (c)'] = 0.0
    final_df['Reimb Pay (e)'] = 0.0
    
    final_df['Bill Rate'] = df['bill_rate']
    final_df['Total Bill'] = df['Total Bill'].round(2)
    
    final_df['Status'] = df['status']
    final_df['Cancellation Reason'] = df['cancellation_reason']
    
    # Verification columns
    # logic: if start_verified_at is set, use 'Verified', else 'Unverified' or actual values?
    # PHP uses t.start_verified (boolean?) or timestamps?
    # Logic in PHP was specialized. Here we just dump what we have or 'Verified'/'Pending'
    # The SQL selected verification_start/end etc.
    final_df['Verification (c)'] = df.apply(lambda r: 'Verified' if r['verification_start_at'] else 'Pending', axis=1)
    final_df['Verification (e)'] = df.apply(lambda r: 'Verified' if r['verification_end_at'] else 'Pending', axis=1)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="timesheet_verification")
    output.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="timesheet_verification_report.xlsx"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

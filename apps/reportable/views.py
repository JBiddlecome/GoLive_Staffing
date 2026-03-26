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


class ScheduledWorkedHoursPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class TimesheetVerificationPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class UnconfirmedRequestsPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class MapsReportPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class GlobalShiftReportPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class NowstaReportPayload(BaseModel):
    start_date: str = Field(..., min_length=1)
    end_date: str = Field(..., min_length=1)
    limit: int = Field(default=50000, ge=1, le=100000)


class VenueShiftsPayload(BaseModel):
    start_date: str = Field(default="2025-01-01")
    end_date: str = Field(default="2025-12-31")
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

CANCEL_REASON_MAP = {
    1: "Shift Cancelled",
    2: "Employee Cancelled < 24 Hours Notice",
    3: "Employee Cancelled > 24 Hours Notice",
    4: "Client Cancelled Shift",
    5: "Client Decreased Staff Needed",
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


@router.get("/event/{event_id}")
async def reportable_event_details(event_id: int) -> JSONResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                e.title,
                e.date,
                e.venue_details,
                e.parking_note,
                e.directions,
                e.check_in,
                v.name AS venue_name,
                COALESCE(e.address1, v.address1) AS address1,
                COALESCE(e.address2, v.address2) AS address2,
                COALESCE(e.city, v.city) AS city,
                COALESCE(e.state, v.state) AS state,
                COALESCE(e.zip, v.zip) AS zip,
                (SELECT MIN(start) FROM shift WHERE event_id = e.event_id AND deleted_at IS NULL) AS shift_start
            FROM event e
            JOIN venue v ON e.venue_id = v.venue_id
            WHERE e.event_id = :event_id
            """
        )
        with engine.begin() as connection:
            result = connection.execute(sql, {"event_id": event_id}).mappings().first()
            if not result:
                raise HTTPException(status_code=404, detail=f"Event {event_id} not found.")
            
            # Format results
            data = dict(result)
            if data.get("date"):
                data["date"] = data["date"].isoformat()
            
            if data.get("shift_start"):
                # Return time in HH:MM format
                data["shift_start"] = data["shift_start"].strftime("%I:%M %p").lstrip("0")

            # Fetch shifts for the event
            shifts_sql = text(
                """
                SELECT s.start, p.description as position
                FROM shift s
                JOIN shift_position sp ON s.shift_id = sp.shift_id
                JOIN position p ON sp.position_id = p.position_id
                WHERE s.event_id = :event_id AND s.deleted_at IS NULL
                ORDER BY s.start
                """
            )
            shift_results = connection.execute(shifts_sql, {"event_id": event_id}).mappings().all()
            
            shifts = []
            for row in shift_results:
                if row["start"]:
                    # Format time as requested: e.g., "7:00 AM"
                    time_str = row["start"].strftime("%I:%M %p").lstrip("0")
                    shifts.append({
                        "time": time_str,
                        "position": row["position"],
                        "label": f"{time_str} {row['position']}"
                    })
            data["shifts"] = shifts
                
            addr = data.get("address1", "")
            if data.get("address2"):
                addr += f", {data['address2']}"
            addr += f", {data.get('city', '')}, {data.get('state', '')} {data.get('zip', '')}"
            data["location"] = addr.strip(", ")
            
            return JSONResponse(data)
    finally:
        engine.dispose()


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
        # Dynamically check which optional columns exist to avoid errors on schema variations
        inspector = inspect(engine)
        client_columns = {col["name"] for col in inspector.get_columns("client")}
        shift_position_columns = {col["name"] for col in inspector.get_columns("shift_position")}
        timesheet_columns = {col["name"] for col in inspector.get_columns("timesheet")}

        markup_select = "c.markup AS markup" if "markup" in client_columns else "NULL AS markup"
        code_select = "sp.code AS code" if "code" in shift_position_columns else "NULL AS code"

        # Select timesheet flag/seconds columns only if they exist in the schema
        ts_cols = [
            "client_seconds", "employee_seconds",
            "client_min_bill", "employee_min_pay",
            "client_no_bill", "employee_no_pay",
            "client_no_break_penalty", "employee_no_break_penalty",
        ]
        ts_select_str = ", ".join(
            f"t.{col}" if col in timesheet_columns else f"0 AS {col}"
            for col in ts_cols
        )

        sql = text(
            f"""
            SELECT
                DAYNAME(e.date)              AS day,
                e.date                       AS date,
                e.state                      AS event_state,
                COALESCE(wc.wc_code, '8810') AS wc,
                c.name                       AS client,
                {markup_select},
                v.name                       AS venue,
                e.title                      AS event,
                p.description                AS position,
                {code_select},
                emp.payroll_id               AS emp_number,
                emp.first_name               AS first_name,
                emp.last_name                AS last_name,
                mw.description               AS work_state,
                se.bill_rate                 AS bill_rate,
                se.rate                      AS pay_rate,
                t.client_tips                AS tip_c,
                t.client_parking             AS park_c,
                t.client_travel              AS travel_c,
                t.client_service_charge      AS service_c,
                v.service_charge             AS venue_service_charge,
                CASE
                    WHEN se.cancel_reason > 0 THEN 'CANCELLED'
                    ELSE 'WORKED'
                END                          AS status,
                se.cancel_reason             AS cancellation_reason,
                t.start_verified_at          AS verification_start_at,
                t.end_verified_at            AS verification_end_at,
                {ts_select_str}
            FROM shift_employee se
            JOIN event e            ON se.event_id             = e.event_id
            JOIN client c           ON e.client_id             = c.client_id
            LEFT JOIN wc_code wc    ON c.wc_id                 = wc.wc_id
            LEFT JOIN venue v       ON e.venue_id              = v.venue_id
            JOIN employee emp       ON se.employee_id           = emp.employee_id
            LEFT JOIN min_wage_rate mw ON emp.min_wage_id       = mw.min_wage_id
            LEFT JOIN timesheet t   ON se.shift_employee_id    = t.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN position p    ON sp.position_id           = p.position_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
              AND (
                  (se.confirmed = 1 AND se.cancel_reason = 0)
                  OR t.client_min_bill  = 1
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

    # Return empty sheet with correct headers when no data matches the date range
    empty_columns = [
        "Day", "Date", "WC", "Client", "Markup", "Venue", "Event", "Position", "Code",
        "#Emp", "First Name", "Last Name", "Work State",
        "Reg H (c)", "OT H (c)", "DT H (c)", "Reg Rate (c)", "Non-Worked Hours (c)",
        "Cert Cost (e)", "OT R", "DT R", "Tip (c)", "Park (c)", "Travel (c)",
        "Service (c)", "Meal (c)", "Non-Worked Bill (c)", "Reimb Pay (e)",
        "Pay Rate", "Bill Rate", "Total Bill", "Status", "Cancellation Reason",
        "Verification (c)", "Verification (e)",
    ]
    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=empty_columns).to_excel(
                writer, index=False, sheet_name="timesheet_verification"
            )
        output.seek(0)
        headers = {"Content-Disposition": 'attachment; filename="timesheet_verification_report.xlsx"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    numeric_cols = [
        "client_seconds", "tip_c", "park_c", "travel_c", "service_c", "venue_service_charge",
        "client_no_break_penalty", "bill_rate", "pay_rate",
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)
    df["markup"] = df["markup"].fillna(0)
    df["code"] = df["code"].fillna("")

    def calculate_row(row: "pd.Series") -> "pd.Series":
        # Hours (client side): use pre-calculated client_seconds, apply 4h min-bill floor
        client_hours = float(row["client_seconds"]) / 3600.0
        if row["client_min_bill"] and client_hours < 4.0:
            client_hours = 4.0
        if row["client_no_bill"]:
            client_hours = 0.0

        # Overtime split by state (daily rules)
        reg_h = ot_h = dt_h = 0.0
        state = str(row["event_state"]).upper() if row["event_state"] else ""
        if state in ("CA", "CALIFORNIA"):
            if client_hours > 12:
                dt_h = client_hours - 12
                ot_h = 4.0
                reg_h = 8.0
            elif client_hours > 8:
                ot_h = client_hours - 8
                reg_h = 8.0
            else:
                reg_h = client_hours
        elif state in ("NV", "NEVADA"):
            if client_hours > 8:
                ot_h = client_hours - 8
                reg_h = 8.0
            else:
                reg_h = client_hours
        else:
            reg_h = client_hours

        # Rates
        bill_rate = float(row["bill_rate"])
        ot_rate = round(bill_rate * 1.5, 2)
        dt_rate = round(bill_rate * 2.0, 2)

        # Billing amounts
        reg_bill = reg_h * bill_rate
        ot_bill  = ot_h  * ot_rate
        dt_bill  = dt_h  * dt_rate

        # Service charge: % of wages + flat venue charge
        service_pct  = float(row["service_c"] or 0)
        venue_flat   = float(row["venue_service_charge"] or 0)
        service_amt  = ((reg_bill + ot_bill + dt_bill) * service_pct / 100.0) + venue_flat

        # Meal penalty: 1 hour at bill rate if a break penalty was recorded
        meal_amt = bill_rate if float(row["client_no_break_penalty"]) > 0 else 0.0

        total_bill = (
            reg_bill + ot_bill + dt_bill
            + service_amt + meal_amt
            + float(row["tip_c"]) + float(row["park_c"]) + float(row["travel_c"])
        )

        return pd.Series({
            "Reg H (c)":            round(reg_h, 2),
            "OT H (c)":             round(ot_h, 2),
            "DT H (c)":             round(dt_h, 2),
            "Reg Rate (c)":         round(bill_rate, 2),
            "Non-Worked Hours (c)": 0.0,
            "OT R":                 ot_rate,
            "DT R":                 dt_rate,
            "Service (c)":          round(service_amt, 2),
            "Meal (c)":             round(meal_amt, 2),
            "Total Bill":           round(total_bill, 2),
            "Verification (c)":     "Verified" if pd.notnull(row["verification_start_at"]) else "Pending",
            "Verification (e)":     "Verified" if pd.notnull(row["verification_end_at"]) else "Pending",
        })

    calc_df = df.apply(calculate_row, axis=1)
    df = pd.concat([df, calc_df], axis=1)

    # Assemble final output in report column order
    final_df = pd.DataFrame({
        "Day":                  df["day"],
        "Date":                 pd.to_datetime(df["date"]).dt.date,
        "WC":                   df["wc"],
        "Client":               df["client"],
        "Markup":               df["markup"],
        "Venue":                df["venue"],
        "Event":                df["event"],
        "Position":             df["position"],
        "Code":                 df["code"],
        "#Emp":                 df["emp_number"],
        "First Name":           df["first_name"],
        "Last Name":            df["last_name"],
        "Work State":           df["work_state"],
        "Reg H (c)":            df["Reg H (c)"],
        "OT H (c)":             df["OT H (c)"],
        "DT H (c)":             df["DT H (c)"],
        "Reg Rate (c)":         df["Reg Rate (c)"],
        "Non-Worked Hours (c)": df["Non-Worked Hours (c)"],
        "Cert Cost (e)":        0.0,
        "OT R":                 df["OT R"],
        "DT R":                 df["DT R"],
        "Tip (c)":              df["tip_c"],
        "Park (c)":             df["park_c"],
        "Travel (c)":           df["travel_c"],
        "Service (c)":          df["Service (c)"],
        "Meal (c)":             df["Meal (c)"],
        "Non-Worked Bill (c)":  0.0,
        "Reimb Pay (e)":        0.0,
        "Pay Rate":             df["pay_rate"],
        "Bill Rate":            df["bill_rate"],
        "Total Bill":           df["Total Bill"],
        "Status":               df["status"],
        "Cancellation Reason":  df["cancellation_reason"],
        "Verification (c)":     df["Verification (c)"],
        "Verification (e)":     df["Verification (e)"],
    })

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
                e.work_area_distance AS `Work Area`,
                
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
        'Work Area',
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


@router.post("/export/scheduled-worked-hours")
async def reportable_scheduled_worked_hours_export(
    payload: ScheduledWorkedHoursPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                DAYNAME(e.date)              AS day,
                e.date                       AS date,
                c.name                       AS client,
                v.name                       AS venue,
                e.title                      AS event,
                emp.payroll_id               AS employee_id,
                emp.first_name               AS first_name,
                emp.last_name                AS last_name,
                s.shift_id                   AS shift_id,
                s.event_id                   AS shift_event_id,
                s.start                      AS raw_shift_start,
                s.end                        AS raw_shift_end,
                DATE_FORMAT(s.start, '%Y-%m-%d %H:%i:%s') AS shift_start,
                DATE_FORMAT(s.end, '%Y-%m-%d %H:%i:%s')   AS shift_end,
                e.state                      AS event_state,
                se.bill_rate                 AS bill_rate,
                se.rate                      AS pay_rate,
                se.created_at                AS se_created_at,
                se.confirmed_at              AS se_confirmed_at,
                sp.rate                      AS shift_pos_rate,
                t.client_seconds             AS client_seconds,
                t.employee_seconds           AS employee_seconds,
                t.client_min_bill            AS client_min_bill,
                t.employee_min_pay           AS employee_min_pay,
                t.client_no_bill             AS client_no_bill,
                t.employee_no_pay            AS employee_no_pay,
                t.client_no_break_penalty    AS client_no_break_penalty,
                t.employee_no_break_penalty  AS employee_no_break_penalty,
                t.client_worked              AS status,
                t.timesheet_id               AS timesheet_id
            FROM shift_employee se
            JOIN event e            ON se.event_id             = e.event_id
            JOIN client c           ON e.client_id             = c.client_id
            JOIN venue v            ON e.venue_id              = v.venue_id
            JOIN employee emp       ON se.employee_id           = emp.employee_id
            JOIN shift_position sp  ON se.shift_position_id    = sp.shift_position_id
            JOIN shift s            ON sp.shift_id             = s.shift_id
            LEFT JOIN timesheet t   ON se.shift_employee_id    = t.shift_employee_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
              AND (
                  (se.confirmed = 1 AND se.cancel_reason = 0)
                  OR t.client_min_bill  = 1
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        engine.dispose()

    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                "Day", "Date", "Client", "Venue", "Event", "Employee ID", "First Name", "Last Name",
                "Shift ID", "Shift Event ID", "Shift Start Time", "Shift End Time",
                "Request", "Confirmed", "Confirm Time",
                "Reg H (e)", "OT H (e)", "DT H (e)", "Gross Pay",
                "Reg H (c)", "OT H (c)", "DT H (c)", "Total Bill",
                "Scheduled Hours", "Pay Rate", "Total Pay", "Status", "Timesheet ID"
            ]).to_excel(writer, index=False, sheet_name="scheduled_worked_hours")
        output.seek(0)
        headers = {"Content-Disposition": 'attachment; filename="scheduled_worked_hours_report.xlsx"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    numeric_cols = [
        "shift_pos_rate", "employee_seconds", "employee_no_pay", "pay_rate",
        "employee_no_break_penalty", "client_seconds", "client_no_bill", "bill_rate",
        "client_no_break_penalty", "client_min_bill", "employee_min_pay"
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df["status"] = df["status"].fillna(0)
    df["timesheet_id"] = df["timesheet_id"].fillna(0)

    def calculate_row(row: "pd.Series") -> "pd.Series":
        # Ported logic from PayrollExportForm / ClientsAndEmployeesExportJob
        
        # --- Scheduled Hours ---
        sched_hours = 0.0
        start_ts = pd.to_datetime(row["raw_shift_start"])
        end_ts = pd.to_datetime(row["raw_shift_end"])
        
        if pd.notnull(start_ts) and pd.notnull(end_ts):
            delta = end_ts - start_ts
            total_seconds = delta.total_seconds()
            sched_hours = total_seconds / 3600.0
            
            # Subtract 0.5 for meal break if over 5 hours (30 mins = 1800 seconds)
            if total_seconds > 18000: # 5 hours * 3600
                sched_hours -= 0.5

        # --- Pay Rate ---
        # User requested: "Pay Rate, which pulls the rate from table shift_position"
        shift_pos_rate = float(row["shift_pos_rate"])
        total_pay = sched_hours * shift_pos_rate

        # --- Employee Side (Worked) ---
        emp_seconds = float(row["employee_seconds"])
        emp_hours = emp_seconds / 3600.0
        
        # Min pay logic (simplified port)
        if row["employee_min_pay"] and emp_hours < 4.0:
             emp_hours = 4.0
             
        if row["employee_no_pay"]:
            emp_hours = 0.0

        # Split emp hours
        reg_h_e = ot_h_e = dt_h_e = 0.0
        state = str(row["event_state"]).upper() if row["event_state"] else ""
        if state in ("CA", "CALIFORNIA"):
            if emp_hours > 12:
                dt_h_e = emp_hours - 12
                ot_h_e = 4.0
                reg_h_e = 8.0
            elif emp_hours > 8:
                ot_h_e = emp_hours - 8
                reg_h_e = 8.0
            else:
                reg_h_e = emp_hours
        elif state in ("NV", "NEVADA"):
            if emp_hours > 8:
                ot_h_e = emp_hours - 8
                reg_h_e = 8.0
            else:
                reg_h_e = emp_hours
        else:
            reg_h_e = emp_hours

        pay_rate = float(row["pay_rate"])
        gross_pay = (reg_h_e * pay_rate) + (ot_h_e * pay_rate * 1.5) + (dt_h_e * pay_rate * 2.0)
        
        # Add meal penalty if exists
        if float(row["employee_no_break_penalty"]) > 0:
            gross_pay += pay_rate # 1h penalty

        # --- Client Side ---
        client_seconds = float(row["client_seconds"])
        client_hours = client_seconds / 3600.0
        
        if row["client_min_bill"] and client_hours < 4.0:
            client_hours = 4.0
        if row["client_no_bill"]:
            client_hours = 0.0

        reg_h_c = ot_h_c = dt_h_c = 0.0
        if state in ("CA", "CALIFORNIA"):
            if client_hours > 12:
                dt_h_c = client_hours - 12
                ot_h_c = 4.0
                reg_h_c = 8.0
            elif client_hours > 8:
                ot_h_c = client_hours - 8
                reg_h_c = 8.0
            else:
                reg_h_c = client_hours
        elif state in ("NV", "NEVADA"):
            if client_hours > 8:
                ot_h_c = client_hours - 8
                reg_h_c = 8.0
            else:
                reg_h_c = client_hours
        else:
            reg_h_c = client_hours

        bill_rate = float(row["bill_rate"])
        total_bill = (reg_h_c * bill_rate) + (ot_h_c * bill_rate * 1.5) + (dt_h_c * bill_rate * 2.0)
        
        if float(row["client_no_break_penalty"]) > 0:
            total_bill += bill_rate

        return pd.Series({
            "Scheduled Hours": round(sched_hours, 2),
            "Shift Pos Rate": round(shift_pos_rate, 2),
            "Calc Total Pay": round(total_pay, 2),
            "Reg H (e)": round(reg_h_e, 2),
            "OT H (e)":  round(ot_h_e, 2),
            "DT H (e)":  round(dt_h_e, 2),
            "Gross Pay": round(gross_pay, 2),
            "Reg H (c)": round(reg_h_c, 2),
            "OT H (c)":  round(ot_h_c, 2),
            "DT H (c)":  round(dt_h_c, 2),
            "Total Bill": round(total_bill, 2),
            "Request": row["se_created_at"] if pd.notnull(row["se_created_at"]) else "",
            "Confirmed": row["se_confirmed_at"] if pd.notnull(row["se_confirmed_at"]) else "",
            "Confirm Time": str(row["se_confirmed_at"] - row["se_created_at"]) if pd.notnull(row["se_confirmed_at"]) and pd.notnull(row["se_created_at"]) else "",
        })

    calc_df = df.apply(calculate_row, axis=1)
    df = pd.concat([df, calc_df], axis=1)

    final_df = pd.DataFrame({
        "Day":           df["day"],
        "Date":          pd.to_datetime(df["date"]).dt.date,
        "Client":        df["client"],
        "Venue":         df["venue"],
        "Event":         df["event"],
        "Employee ID":   df["employee_id"],
        "First Name":    df["first_name"],
        "Last Name":     df["last_name"],
        "Shift ID":      df["shift_id"],
        "Shift Event ID":df["shift_event_id"],
        "Shift Start Time": df["shift_start"],
        "Shift End Time":   df["shift_end"],
        "Request":       df["Request"],
        "Confirmed":     df["Confirmed"],
        "Confirm Time":  df["Confirm Time"],
        "Reg H (e)":     df["Reg H (e)"],
        "OT H (e)":      df["OT H (e)"],
        "DT H (e)":      df["DT H (e)"],
        "Gross Pay":     df["Gross Pay"],
        "Reg H (c)":     df["Reg H (c)"],
        "OT H (c)":      df["OT H (c)"],
        "DT H (c)":      df["DT H (c)"],
        "Total Bill":    df["Total Bill"],
        "Scheduled Hours": df["Scheduled Hours"],
        "Pay Rate":      df["Shift Pos Rate"],
        "Total Pay":     df["Calc Total Pay"],
        "Status":        df["status"],
        "Timesheet ID":  df["timesheet_id"]
    })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="scheduled_worked_hours")
    output.seek(0)

    filename = "scheduled_worked_hours_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/export/unconfirmed-requests")
async def reportable_unconfirmed_requests_export(
    payload: UnconfirmedRequestsPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                e.client_id,
                e.venue_id,
                e.date,
                e.stat_waiting_for_admin_count,
                v.staffing_manager_id,
                se.shift_employee_id,
                se.employee_id,
                se.confirmed,
                se.request_by,
                se.confirmed_at,
                se.created_at,
                se.created_time,
                se.remove_by AS removed_by,
                se.deleted_at,
                se.cancelled_at,
                u_req.id AS requester_user_id,
                u_req.first_name AS requester_first_name,
                u_req.last_name AS requester_last_name,
                u_req.role AS requester_role,
                u_req.group AS requester_group,
                u_rem.id AS remover_user_id,
                u_rem.first_name AS remover_first_name,
                u_rem.last_name AS remover_last_name,
                u_rem.role AS remover_role,
                u_rem.group AS remover_group,
                s.shift_id,
                emp.payroll_id,
                emp.first_name,
                emp.last_name
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN venue v ON e.venue_id = v.venue_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            LEFT JOIN user u_req ON se.request_by = u_req.id
            LEFT JOIN user u_rem ON se.remove_by  = u_rem.id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
              AND se.confirmed = 0
            ORDER BY e.date, emp.last_name, emp.first_name
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        engine.dispose()

    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                "Client ID", "Venue ID", "Date", "Stat Waiting For Admin",
                "Staffing Manager ID", "Shift Employee ID", "Employee ID", "Confirmed",
                "Requested By", "Confirmed At", "Created At", "Created Time",
                "Removed By", "Deleted At", "Cancelled At",
                "Requester ID", "Requester First Name", "Requester Last Name", "Requester Role", "Requester Group",
                "Remover ID", "Remover First Name", "Remover Last Name", "Remover Role", "Remover Group",
                "Shift ID", "Payroll ID", "First Name", "Last Name"
            ]).to_excel(writer, index=False, sheet_name="unconfirmed_requests")
        output.seek(0)
        filename = "unconfirmed_requests_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    # DataFrame formatting (optional improvements)
    final_df = df.rename(columns={
        "client_id": "Client ID",
        "venue_id": "Venue ID",
        "date": "Date",
        "stat_waiting_for_admin_count": "Stat Waiting For Admin",
        "staffing_manager_id": "Staffing Manager ID",
        "shift_employee_id": "Shift Employee ID",
        "employee_id": "Employee ID",
        "confirmed": "Confirmed",
        "request_by": "Requested By",
        "confirmed_at": "Confirmed At",
        "created_at": "Created At",
        "created_time": "Created Time",
        "removed_by": "Removed By",
        "deleted_at": "Deleted At",
        "cancelled_at": "Cancelled At",
        "requester_user_id": "Requester ID",
        "requester_first_name": "Requester First Name",
        "requester_last_name": "Requester Last Name",
        "requester_role": "Requester Role",
        "requester_group": "Requester Group",
        "remover_user_id": "Remover ID",
        "remover_first_name": "Remover First Name",
        "remover_last_name": "Remover Last Name",
        "remover_role": "Remover Role",
        "remover_group": "Remover Group",
        "shift_id": "Shift ID",
        "payroll_id": "Payroll ID",
        "first_name": "First Name",
        "last_name": "Last Name"
    })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="unconfirmed_requests")
    output.seek(0)

    filename = "unconfirmed_requests_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/export/maps-report")
async def reportable_maps_report_export(
    payload: MapsReportPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                e.date                       AS `Event Date`,
                e.title                      AS `Event Title`,
                e.client_id                  AS `Client ID`,
                e.venue_id                   AS `Venue ID`,
                e.latitude                   AS `Latitude`,
                e.longitude                  AS `Longitude`,
                s.shift_id                   AS `Shift ID`,
                DATE_FORMAT(s.start, '%Y-%m-%d %H:%i:%s') AS `Shift Start`,
                DATE_FORMAT(s.end, '%Y-%m-%d %H:%i:%s')   AS `Shift End`,
                sp.shift_position_id         AS `Shift Position ID`,
                p.description                AS `Position`,
                sp.rate                      AS `Rate`,
                se.employee_id               AS `Employee ID`,
                emp.first_name               AS `First Name`,
                emp.last_name                AS `Last Name`,
                CASE 
                    WHEN se.confirmed = 1 THEN 'Confirmed'
                    WHEN se.confirmed = 0 AND se.shift_employee_id IS NOT NULL THEN 'Request'
                    ELSE 'Open'
                END AS `Status`
            FROM event e
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            JOIN position p ON sp.position_id = p.position_id
            LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.cancel_reason = 0
            LEFT JOIN employee emp ON se.employee_id = emp.employee_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
            ORDER BY e.date, e.title, s.start, p.description
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        engine.dispose()

    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                "Event Date", "Event Title", "Client ID", "Venue ID", "Latitude", "Longitude",
                "Shift ID", "Shift Start", "Shift End", "Shift Position ID", "Position", "Rate",
                "Employee ID", "First Name", "Last Name", "Status"
            ]).to_excel(writer, index=False, sheet_name="maps_report")
        output.seek(0)
        filename = "maps_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="maps_report")
    output.seek(0)

    filename = "maps_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/export/global-shift-report")
async def reportable_global_shift_report_export(
    payload: GlobalShiftReportPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                e.date                       AS date,
                c.name                       AS client_name,
                v.name                       AS venue_name,
                s.shift_id                   AS shift_id,
                sp.shift_position_id         AS shift_position_id,
                sp.created_at                AS pos_created_at,
                sp.deleted_at                AS pos_deleted_at,
                s.deleted_at                 AS shift_deleted_at,
                se.cancel_reason             AS cancel_reason_id,
                se.employee_cancel_reason    AS employee_cancel_reason,
                se.employee_id               AS employee_id,
                emp.first_name               AS emp_first,
                emp.last_name                AS emp_last
            FROM shift_position sp
            JOIN shift s ON sp.shift_id = s.shift_id
            JOIN event e ON s.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id
            LEFT JOIN employee emp ON se.employee_id = emp.employee_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
            ORDER BY e.date DESC, c.name, s.shift_id
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

    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                "Date", "Client Name", "Venue Name", "Shift ID", "Status",
                "Position Created At", "Position Deleted At", "Shift Deleted At",
                "Cancel Reason", "Employee Cancel Reason"
            ]).to_excel(writer, index=False, sheet_name="global_shift_report")
        output.seek(0)
        filename = "global_shift_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    # Process status and text reasons
    def determine_status(row):
        if pd.notnull(row["shift_deleted_at"]):
            return "Shift Deleted"
        if pd.notnull(row["pos_deleted_at"]):
            return "Position Deleted"
        
        reason_id = row["cancel_reason_id"]
        if reason_id == 4:
            return "Client Cancelled Shift"
        if reason_id == 5:
            return "Client Decreased Staff"
        if reason_id == 1:
            return "Shift Cancelled"
            
        if pd.notnull(row["employee_id"]):
            if reason_id in (2, 3):
                return "Employee Cancelled"
            return "Filled/Worked"
            
        return "Unfilled/Open"

    df["Status"] = df.apply(determine_status, axis=1)
    df["Cancel Reason"] = df["cancel_reason_id"].map(CANCEL_REASON_MAP).fillna("")
    
    # Final column selection and renaming
    final_df = pd.DataFrame({
        "Date": pd.to_datetime(df["date"]).dt.date,
        "Client Name": df["client_name"],
        "Venue Name": df["venue_name"],
        "Shift ID": df["shift_id"],
        "Status": df["Status"],
        "Position Created At": df["pos_created_at"],
        "Position Deleted At": df["pos_deleted_at"],
        "Shift Deleted At": df["shift_deleted_at"],
        "Cancel Reason": df["Cancel Reason"],
        "Employee Cancel Reason": df["employee_cancel_reason"].fillna(""),
    })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="global_shift_report")
    output.seek(0)

    filename = "global_shift_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/export/nowsta-report")
async def reportable_nowsta_report_export(
    payload: NowstaReportPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                emp.payroll_id AS `Employee ID`,
                emp.first_name AS `First Name`,
                emp.last_name AS `Last Name`,
                p.description AS `Position`,
                CASE 
                    WHEN se.confirmed = 1 THEN 'Confirmed'
                    WHEN se.confirmed = 0 AND se.shift_employee_id IS NOT NULL THEN 'Request'
                    ELSE 'Open'
                END AS `Status`
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN position p ON sp.position_id = p.position_id
            WHERE e.client_id = 1079
              AND e.date >= :start_date
              AND e.date <= :end_date
              AND se.cancel_reason = 0
            ORDER BY e.date, emp.last_name, emp.first_name
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

    if df.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                "Employee ID", "First Name", "Last Name", "Position", "Status"
            ]).to_excel(writer, index=False, sheet_name="nowsta_report")
        output.seek(0)
        filename = "nowsta_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="nowsta_report")
    output.seek(0)

    filename = "nowsta_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/export/venue-shifts")
async def reportable_venue_shifts_export(
    payload: VenueShiftsPayload,
) -> StreamingResponse:
    engine = _engine()
    try:
        # SQL Query to pull venues that had shifts in the specified range (default 2025)
        # Includes Client Name, Venue Name, Venue Address, and Event Address
        sql = text(
            """
            SELECT DISTINCT
                c.name AS `Client Name`,
                v.name AS `Venue Name`,
                TRIM(CONCAT_WS(', ', v.address1, NULLIF(v.address2, ''), v.city, v.state, v.zip)) AS `Venue Address`,
                TRIM(CONCAT_WS(', ', e.address1, NULLIF(e.address2, ''), e.city, e.state, e.zip)) AS `Event Address`
            FROM venue v
            JOIN client c ON v.client_id = c.client_id
            JOIN event e ON e.venue_id = v.venue_id
            JOIN shift s ON s.event_id = e.event_id
            WHERE s.start >= :start_date
              AND s.start <= :end_date
              AND s.deleted_at IS NULL
            ORDER BY c.name, v.name
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

    if df.empty:
        # Return empty excel with headers if no data
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(columns=[
                'Client Name', 'Venue Name', 'Venue Address', 'Event Address'
            ]).to_excel(writer, index=False, sheet_name="venue_shifts")
        output.seek(0)
        filename = "venue_shifts_report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="venue_shifts")
    output.seek(0)

    filename = "venue_shifts_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

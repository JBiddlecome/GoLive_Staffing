import os
import pandas as pd
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from datetime import date

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
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

@router.get("", response_class=HTMLResponse)
async def profit_tracker_page(request: Request):
    today = date.today().isoformat()
    return templates.TemplateResponse(
        "apps/profit_tracker.html", 
        {"request": request, "start_date": today, "end_date": today}
    )

class ProfitPayload(BaseModel):
    start_date: str
    end_date: str

@router.post("/api/data")
async def get_profit_data(payload: ProfitPayload):
    engine = _engine()
    try:
        inspector = inspect(engine)
        timesheet_columns = {col["name"] for col in inspector.get_columns("timesheet")}

        ts_cols = [
            "use_sheet",
            "client_seconds", "employee_seconds",
            "client_min_bill", "employee_min_pay",
            "client_no_bill", "employee_no_pay",
            "client_no_break_penalty", "employee_no_break_penalty",
            "client_tips", "client_parking", "client_travel", "client_service_charge",
            "employee_tips", "employee_parking", "employee_travel", "employee_service_charge"
        ]
        ts_select_str = ", ".join(
            f"t.{col}" if col in timesheet_columns else f"0 AS {col}"
            for col in ts_cols
        )

        sql = text(
            f"""
            SELECT
                e.date,
                sp.bonus,
                c.name AS client_name,
                se.bill_rate,
                se.rate AS pay_rate,
                v.service_charge AS venue_service_charge,
                m.rate AS msp_rate,
                wc.rate AS wc_rate,
                e.state AS event_state,
                t.client_worked,
                t.employee_worked,
                s.start AS shift_start,
                t.client_start,
                t.employee_start,
                {ts_select_str}
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN msp m ON c.msp_id = m.id
            LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN shift s ON sp.shift_id = s.shift_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND (
                  (se.confirmed = 1 AND se.cancel_reason = 0)
                  OR t.client_min_bill = 1
                  OR t.employee_min_pay = 1
              )
            """
        )

        params = {"start_date": payload.start_date, "end_date": payload.end_date}

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)
            
            try:
                asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
                asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
                asp_rules = [dict(r) for r in asp_rules_raw]
            except Exception:
                asp_rules = []
            
            other_work_sql = text(
                """
                SELECT SUM(cost) as total_other_work
                FROM employee_other_work
                WHERE date >= :start_date AND date <= :end_date
                """
            )
            other_work_res = connection.execute(other_work_sql, params).scalar()
            other_work_sum = float(other_work_res) if other_work_res else 0.0

    finally:
        engine.dispose()

    if df.empty:
        return JSONResponse({
            "total_bill": 0,
            "gross_pay": 0,
            "msp_fee": 0,
            "wc_fee": 0,
            "payroll_tax": 0,
            "other_work": 0,
            "profit": 0,
            "client_breakdown": []
        })

    numeric_cols = [
        "client_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    def process_row(row):
        use_sheet = str(row.get("use_sheet") or "").upper()
        c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
        e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0

        # Legacy getUsesBothSheets() = (use_sheet IS NULL)
        # NULL -> client_seconds for billing, employee_seconds for pay (independently)
        # CLIENT/EMPLOYEE -> resolve single seconds, used for both billing and pay
        uses_both_sheets = (use_sheet == "")
        if uses_both_sheets:
            bill_seconds = c_sec
            pay_seconds = e_sec
        elif use_sheet == "EMPLOYEE":
            bill_seconds = e_sec
            pay_seconds = e_sec
        else:  # CLIENT
            bill_seconds = c_sec
            pay_seconds = c_sec

        c_hours = bill_seconds / 3600.0  # hours for billing
        e_hours = pay_seconds / 3600.0   # hours for pay

        c_worked = str(row.get("client_worked") or "").upper()
        e_worked_raw = str(row.get("employee_worked") or "").upper()
        c_min = row.get("client_min_bill")
        e_min = row.get("employee_min_pay")
        state = str(row["event_state"]).upper() if row["event_state"] else ""

        # Note: The overtime JSON stores WEEKLY accumulated hours and is NOT used by
        # ClientsAndEmployeesExportJob.php. That report always uses CA/NV threshold
        # calculations for both pay and billing OT hours and standard rate multipliers.
        
        # Late hours: legacy calculateLateSeconds(false) respects use_sheet
        # use_sheet=EMPLOYEE -> employee late; CLIENT or NULL -> client late
        shift_start = pd.to_datetime(row.get("shift_start")) if pd.notna(row.get("shift_start")) else None
        c_late_hours = 0.0
        if shift_start and pd.notna(row.get("client_start")):
            c_actual = pd.to_datetime(row["client_start"])
            if c_actual > shift_start:
                c_late_hours = (c_actual - shift_start).total_seconds() / 3600.0
        e_late_hours = 0.0
        if shift_start and pd.notna(row.get("employee_start")):
            e_actual = pd.to_datetime(row["employee_start"])
            if e_actual > shift_start:
                e_late_hours = (e_actual - shift_start).total_seconds() / 3600.0
        # Single late_hours value used in both bill and pay reCalculateRegularHours
        late_hours = e_late_hours if use_sheet == "EMPLOYEE" else c_late_hours

        # ── CLIENT BILLING HOURS ──────────────────────────────────────────
        if pd.notna(c_min) and float(c_min) > 0:
            c_bill_reg = 4.0
            if late_hours > 0 and c_hours < c_bill_reg:
                c_bill_reg -= late_hours
            elif c_hours > c_bill_reg:
                c_bill_reg = c_hours
            c_bill_reg = max(c_bill_reg, 2.0)
            c_bill_reg = min(c_bill_reg, 4.0)
        else:
            c_bill_reg = c_hours

        c_non_worked = 0.0
        e_worked = e_worked_raw
        if e_worked in ("SENTHOME", "CANCELLED"):
            c_non_worked = max(c_bill_reg - c_hours, 0.0)
        c_worked_hours = c_bill_reg - c_non_worked  # actual worked billing hours

        # CA rules always applied for billing OT/DT (matching ClientsAndEmployeesExportJob.php)
        c_ot = c_dt = 0.0
        if c_worked_hours > 12:
            c_dt = c_worked_hours - 12
            c_ot = 4.0
            c_reg = 8.0
        elif c_worked_hours > 8:
            c_ot = c_worked_hours - 8
            c_reg = 8.0
        else:
            c_reg = c_worked_hours

        client_no = row.get("client_no_bill")
        if pd.notna(client_no) and float(client_no) > 0:
            c_reg = c_ot = c_dt = c_non_worked = 0.0

        # ── EMPLOYEE PAY HOURS ────────────────────────────────────────────
        if pd.notna(e_min) and float(e_min) > 0:
            e_pay_reg = 4.0
            if late_hours > 0 and e_hours < e_pay_reg:
                e_pay_reg -= late_hours
            elif e_hours > e_pay_reg:
                e_pay_reg = e_hours
            e_pay_reg = max(e_pay_reg, 2.0)
            e_pay_reg = min(e_pay_reg, 4.0)
        else:
            e_pay_reg = e_hours

        e_non_worked = 0.0
        if e_worked in ("SENTHOME", "CANCELLED"):
            e_non_worked = max(e_pay_reg - e_hours, 0.0)
        e_worked_hours = e_pay_reg - e_non_worked

        # CA/NV threshold rules always applied for pay OT/DT (matching ClientsAndEmployeesExportJob.php)
        e_ot = e_dt = 0.0
        if state in ("CA", "CALIFORNIA"):
            if e_worked_hours > 12:
                e_dt = e_worked_hours - 12
                e_ot = 4.0
                e_reg = 8.0
            elif e_worked_hours > 8:
                e_ot = e_worked_hours - 8
                e_reg = 8.0
            else:
                e_reg = e_worked_hours
        elif state in ("NV", "NEVADA"):
            if e_worked_hours > 8:
                e_ot = e_worked_hours - 8
                e_reg = 8.0
            else:
                e_reg = e_worked_hours
        else:
            e_reg = e_worked_hours

        emp_no = row.get("employee_no_pay")
        if pd.notna(emp_no) and float(emp_no) > 0:
            e_reg = e_ot = e_dt = e_non_worked = 0.0

        bill_rate = float(row["bill_rate"])
        pay_rate = float(row["pay_rate"])
        
        # Standard rate multipliers (matching ClientsAndEmployeesExportJob.php lines 675-676, 511-512)
        c_ot_rate = bill_rate * 1.5
        c_dt_rate = bill_rate * 2.0
        e_ot_rate = pay_rate * 1.5
        e_dt_rate = pay_rate * 2.0
        
        reg_bill = c_reg * bill_rate
        ot_bill = c_ot * c_ot_rate
        dt_bill = c_dt * c_dt_rate
        non_worked_bill = c_non_worked * bill_rate
        
        reg_pay = e_reg * pay_rate
        ot_pay = e_ot * e_ot_rate
        dt_pay = e_dt * e_dt_rate
        non_worked_pay = e_non_worked * pay_rate

        service_pct_c = float(row.get("client_service_charge") or 0)
        venue_flat = float(row.get("venue_service_charge") or 0)
        service_amt_c = ((reg_bill + ot_bill + dt_bill) * service_pct_c / 100.0) + venue_flat

        service_pct_e = float(row.get("employee_service_charge") or 0)
        service_amt_e = ((reg_pay + ot_pay + dt_pay) * service_pct_e / 100.0)
        
        meal_amt_c = bill_rate if float(row.get("client_no_break_penalty") or 0) > 0 else 0.0
        meal_amt_e = pay_rate if float(row.get("employee_no_break_penalty") or 0) > 0 else 0.0
        
        additional_pay = 0.0
        if "date" in row and pd.notna(row["date"]):
            row_date = pd.to_datetime(row["date"]).date()
            for rule in asp_rules:
                r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
                r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
                start_ok = (r_start is None) or (r_start <= row_date)
                end_ok = (r_end is None) or (r_end >= row_date)
                if start_ok and end_ok:
                    additional_pay += float(rule["rate"])
                    
        bonus_pay = float(row.get("bonus") or 0.0)
        
        # c_worked already set above
        if c_worked not in ("WORKED", "SENTHOME"):
            service_amt_c = 0.0
            meal_amt_c = 0.0
            c_tips = 0.0
            c_parking = 0.0
            c_travel = 0.0
        else:
            c_tips = float(row.get("client_tips", 0))
            c_parking = float(row.get("client_parking", 0))
            c_travel = float(row.get("client_travel", 0))
            
        # e_worked already set above (e_worked = e_worked_raw)
        if e_worked not in ("WORKED", "SENTHOME"):
            service_amt_e = 0.0
            meal_amt_e = 0.0
            additional_pay = 0.0
            bonus_pay = 0.0
            e_tips = 0.0
            e_parking = 0.0
            e_travel = 0.0
        else:
            e_tips = float(row.get("employee_tips", 0))
            e_parking = float(row.get("employee_parking", 0))
            e_travel = float(row.get("employee_travel", 0))
        
        total_bill = (
            reg_bill + ot_bill + dt_bill + non_worked_bill + service_amt_c + meal_amt_c + 
            c_tips + c_parking + c_travel
        )
        total_pay = (
            reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + 
            e_tips + e_parking + e_travel +
            additional_pay + bonus_pay
        )
        
        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        return pd.Series({
            "client_name": row["client_name"],
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee
        })

    calc_df = df.apply(process_row, axis=1)
    
    total_bill_sum = float(calc_df["total_bill"].sum())
    gross_pay_sum = float(calc_df["total_pay"].sum())
    msp_fee_sum = float(calc_df["msp_fee"].sum())
    wc_fee_sum = float(calc_df["wc_fee"].sum())
    
    payroll_tax = gross_pay_sum * 0.10
    
    profit = total_bill_sum - gross_pay_sum - msp_fee_sum - wc_fee_sum - payroll_tax - other_work_sum
    
    client_group = calc_df.groupby("client_name")[["total_bill", "total_pay", "msp_fee", "wc_fee"]].sum().reset_index()
    client_breakdown = []
    for _, r in client_group.iterrows():
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_tax
        client_breakdown.append({
            "client_name": r["client_name"],
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2)
        })
    
    client_breakdown.sort(key=lambda x: x["total_bill"], reverse=True)
    
    return JSONResponse({
        "total_bill": round(total_bill_sum, 2),
        "gross_pay": round(gross_pay_sum, 2),
        "msp_fee": round(msp_fee_sum, 2),
        "wc_fee": round(wc_fee_sum, 2),
        "payroll_tax": round(payroll_tax, 2),
        "other_work": round(other_work_sum, 2),
        "profit": round(profit, 2),
        "client_breakdown": client_breakdown
    })

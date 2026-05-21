import json
import os
import pandas as pd
from pathlib import Path
from typing import Any, Optional
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from datetime import date
from apps.profit_tracker.billing import calculate_total_bill

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _resolve_data_dir() -> Path:
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/opt/render/project/src/data")
    return Path(__file__).resolve().parent.parent.parent / "data"

BT_TIERS_PATH = _resolve_data_dir() / "bt_client_tiers.json"


def _load_bt_tiers() -> dict:
    if BT_TIERS_PATH.exists():
        try:
            with open(BT_TIERS_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_bt_tiers(tiers: dict) -> None:
    BT_TIERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = BT_TIERS_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(tiers, f, indent=2)
    tmp.replace(BT_TIERS_PATH)

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
    from datetime import timedelta
    today = date.today()
    # Default to previous Monday–Sunday
    days_since_monday = today.weekday()  # Mon=0 … Sun=6
    prev_monday = today - timedelta(days=days_since_monday + 7)
    prev_sunday = prev_monday + timedelta(days=6)
    return templates.TemplateResponse(
        "apps/profit_tracker.html", 
        {"request": request, "start_date": prev_monday.isoformat(), "end_date": prev_sunday.isoformat()}
    )

@router.get("/api/msps")
async def get_msps():
    """Return all MSP names for the filter dropdown."""
    engine = _engine()
    try:
        with engine.begin() as conn:
            sql = text("SELECT id, name FROM msp WHERE id NOT IN (4, 8, 9, 12, 13, 21) ORDER BY name")
            rows = conn.execute(sql).mappings().fetchall()
            return [{"id": r["id"], "name": r["name"]} for r in rows]
    finally:
        engine.dispose()


@router.get("/api/bt-tiers")
async def get_bt_tiers():
    """Return the persisted billing-type tier assignments for all clients."""
    return JSONResponse(_load_bt_tiers())


class BtTierPayload(BaseModel):
    client_name: str
    tier: str


@router.post("/api/bt-tiers")
async def set_bt_tier(payload: BtTierPayload):
    """Persist a billing-type tier assignment for a client."""
    if payload.tier not in ("T1", "T2", "T3"):
        raise HTTPException(status_code=400, detail="Invalid tier. Must be T1, T2, or T3.")
    tiers = _load_bt_tiers()
    tiers[payload.client_name] = payload.tier
    _save_bt_tiers(tiers)
    return JSONResponse({"status": "ok"})


class ProfitPayload(BaseModel):
    start_date: str
    end_date: str
    client_name: Optional[str] = None

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
                c.sales_executive_id,
                c.won_date,
                u.last_name AS sales_executive_last_name,
                COALESCE(m.name, '') AS msp_name,
                c.payment_type,
                c.billing_type_id,
                se.bill_rate,
                se.rate AS pay_rate,
                v.service_charge AS venue_service_charge,
                m.rate AS msp_rate,
                wc.rate AS wc_rate,
                e.state AS event_state,
                t.client_worked,
                t.employee_worked,
                s.start AS shift_start,
                s.end AS shift_end,
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
            LEFT JOIN user u ON c.sales_executive_id = u.id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND (
                  (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
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
                SELECT SUM(cost + (rate * (COALESCE(work_hours, 0) + COALESCE(non_work_hours, 0)))) as total_other_work
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
            "cc_fee": 0,
            "bt_fee": 0,
            "commissions": 0,
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

        # Compute shift scheduling metrics (mirrors Shift::getWorkHours() and getMinBillingHours()).
        # GoLive deducts a 0.5h meal break from the scheduled shift duration when the
        # shift is > 5h (BaseMealBreakPolicy). getMinBillingHours() = getWorkHours() / 2.
        # For SENTHOME, the minimum pay/bill floor is getMinBillingHours() (capped at 4h).
        # For regular WORKED shifts, the standard floor is 4h (for shifts >= 4h) or 2h.
        shift_start_raw = row.get("shift_start")
        shift_end_raw   = row.get("shift_end")
        if pd.notna(shift_start_raw) and pd.notna(shift_end_raw):
            shift_dur_hours = (
                pd.to_datetime(shift_end_raw) - pd.to_datetime(shift_start_raw)
            ).total_seconds() / 3600.0
            # Apply meal break deduction: > 5h shift → subtract 0.5h (matches BaseMealBreakPolicy)
            meal_break_deduction = 0.5 if shift_dur_hours > 5.0 else 0.0
            shift_work_hours = shift_dur_hours - meal_break_deduction
            # SENTHOME minimum = getMinBillingHours() = work_hours / 2, capped at 4h
            shift_senthome_min_hours = min(shift_work_hours / 2.0, 4.0)
            # Standard GoLive MinBillingHoursCalculator: >= 4h shift → 4h min; else 2h min
            shift_min_bill_hours = 4.0 if shift_dur_hours >= 4.0 else 2.0
        else:
            shift_work_hours = 4.0  # safe fallback
            shift_senthome_min_hours = 2.0  # safe fallback (4.0 / 2)
            shift_min_bill_hours = 4.0  # safe fallback

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
        # Billing only uses the minimum floor when client_min_bill is explicitly
        # set on the timesheet (GoLive's manageSentHomeMinBillBy sets this flag).
        # If client_min_bill is NOT set (e.g. this SENTHOME had no min bill check),
        # billing uses actual worked hours — the SENTHOME non-worked portion is then
        # derived from (c_bill_reg - c_hours) below. Do NOT apply senthome_min here
        # unconditionally, as that would alter billing on timesheets where the client
        # chose not to set the minimum bill flag.
        if pd.notna(c_min) and float(c_min) > 0:
            c_bill_reg = shift_min_bill_hours  # standard min floor (4h or 2h)
            if late_hours > 0 and c_hours < c_bill_reg:
                c_bill_reg -= late_hours
            elif c_hours > c_bill_reg:
                c_bill_reg = c_hours
            c_bill_reg = max(c_bill_reg, 2.0)
            c_bill_reg = min(c_bill_reg, shift_min_bill_hours)
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
        # For SENTHOME, GoLive always applies the SENTHOME minimum (= shift work hours / 2,
        # capped at 4h) regardless of whether employee_min_pay is set on the timesheet.
        # This matches ClientsAndEmployeesExportJob.php lines 592-618:
        #   if ($timesheet->employee_min_pay || SENTHOME && CA) → reg_hours = getMinBillingHours()
        #   reg_hours = min(reg_hours, 4)   ← PHP hardcodes 4, but getMinBillingHours() already
        #                                      does work_hours/2 which for short shifts < 8h
        #                                      is always <= 4, so min(x,4) is the cap.
        is_senthome = e_worked in ("SENTHOME",)
        if is_senthome or (pd.notna(e_min) and float(e_min) > 0):
            # Legacy PHP ClientsAndEmployeesExportJob uses $shift->getMinBillingHours()
            # (= work_hours/2 capped at 4h) as the floor for ALL min-pay cases —
            # both SENTHOME and employee_min_pay=1 (e.g. CANCELLED with min pay set).
            # shift_senthome_min_hours already encodes min(work_hours/2, 4.0).
            e_pay_reg_floor = shift_senthome_min_hours
            e_pay_reg = e_pay_reg_floor
            if late_hours > 0 and e_hours < e_pay_reg:
                e_pay_reg -= late_hours
            elif e_hours > e_pay_reg:
                e_pay_reg = e_hours
            e_pay_reg = max(e_pay_reg, 2.0)
            e_pay_reg = min(e_pay_reg, e_pay_reg_floor)
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
        
        # GoLive ShiftCalculation.php lines 260-266:
        # bonus and additional pay are only added when BOTH client AND employee "worked"
        # (i.e., client_worked AND employee_worked are WORKED or SENTHOME)
        c_worked = str(row.get("client_worked") or "").upper()
        both_worked = (
            c_worked in ("WORKED", "SENTHOME")
            and e_worked in ("WORKED", "SENTHOME")
        )

        additional_pay = 0.0
        if both_worked and "date" in row and pd.notna(row["date"]):
            row_date = pd.to_datetime(row["date"]).date()
            for rule in asp_rules:
                r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
                r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
                start_ok = (r_start is None) or (r_start <= row_date)
                end_ok = (r_end is None) or (r_end >= row_date)
                if start_ok and end_ok:
                    additional_pay += float(rule["rate"])

        bonus_pay = float(row.get("bonus") or 0.0) if both_worked else 0.0
        
        # Use c_worked for client billing extras
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
        total_bill = calculate_total_bill(row)
        total_pay = (
            reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + 
            e_tips + e_parking + e_travel +
            additional_pay + bonus_pay
        )
        
        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        # Credit card processing fee: 2.9% of total_bill for CC clients (payment_type=1)
        payment_type = row.get("payment_type")
        try:
            cc_fee = total_bill * 0.029 if (pd.notna(payment_type) and int(float(payment_type)) == 1) else 0.0
        except (ValueError, TypeError):
            cc_fee = 0.0

        # Billing type fee: 2.93% (Tier 2 default) of total_bill for billing_type_id=7 clients
        billing_type = row.get("billing_type_id")
        try:
            has_bt_fee = bool(pd.notna(billing_type) and int(float(billing_type)) in (6, 7, 11))
            bt_fee = total_bill * 0.0293 if has_bt_fee else 0.0
        except (ValueError, TypeError):
            has_bt_fee = False
            bt_fee = 0.0

        commissions = 0.0
        sales_executive_id = row.get("sales_executive_id")
        if pd.notna(sales_executive_id) and str(sales_executive_id).strip() != "":
            last_name = str(row.get("sales_executive_last_name") or "").strip()
            if last_name == "Inherited by":
                commissions = total_bill * 0.005
            else:
                won_date_raw = row.get("won_date")
                shift_date_raw = row.get("date")
                if pd.notna(won_date_raw) and pd.notna(shift_date_raw):
                    if pd.to_datetime(shift_date_raw) <= pd.to_datetime(won_date_raw) + pd.DateOffset(years=1):
                        commissions = total_bill * 0.03
                    else:
                        commissions = total_bill * 0.01
                else:
                    commissions = total_bill * 0.01

        return pd.Series({
            "client_name": row["client_name"],
            "msp_name": row.get("msp_name", ""),
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee,
            "cc_fee": cc_fee,
            "bt_fee": bt_fee,
            "commissions": commissions,
            "has_bt_fee": has_bt_fee
        })

    calc_df = df.apply(process_row, axis=1)
    
    total_bill_sum = float(calc_df["total_bill"].sum())
    gross_pay_sum = float(calc_df["total_pay"].sum())
    msp_fee_sum = float(calc_df["msp_fee"].sum())
    wc_fee_sum = float(calc_df["wc_fee"].sum())
    cc_fee_sum = float(calc_df["cc_fee"].sum())
    bt_fee_sum = float(calc_df["bt_fee"].sum())
    commissions_sum = float(calc_df["commissions"].sum())
    
    payroll_tax = gross_pay_sum * 0.10
    
    profit = total_bill_sum - gross_pay_sum - msp_fee_sum - wc_fee_sum - cc_fee_sum - bt_fee_sum - commissions_sum - payroll_tax - other_work_sum
    
    # Build a lookup: client_name -> msp_name (take first non-empty value)
    msp_lookup = {}
    for _, row in calc_df.iterrows():
        cname = row["client_name"]
        mname = row.get("msp_name", "")
        if cname not in msp_lookup or (not msp_lookup[cname] and mname):
            msp_lookup[cname] = mname if pd.notna(mname) else ""

    client_group = calc_df.groupby("client_name").agg({
        "total_bill": "sum", "total_pay": "sum", "msp_fee": "sum",
        "wc_fee": "sum", "cc_fee": "sum", "bt_fee": "sum",
        "commissions": "sum", "has_bt_fee": "max"
    }).reset_index()
    client_breakdown = []
    for _, r in client_group.iterrows():
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_cc = float(r["cc_fee"])
        c_bt = float(r["bt_fee"])
        c_commissions = float(r["commissions"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_cc - c_bt - c_commissions - c_tax
        client_breakdown.append({
            "client_name": r["client_name"],
            "msp_name": msp_lookup.get(r["client_name"], ""),
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "cc_fee": round(c_cc, 2),
            "bt_fee": round(c_bt, 2),
            "commissions": round(c_commissions, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2),
            "has_bt_fee": bool(r["has_bt_fee"])
        })
    
    client_breakdown.sort(key=lambda x: x["total_bill"], reverse=True)
    
    return JSONResponse({
        "total_bill": round(total_bill_sum, 2),
        "gross_pay": round(gross_pay_sum, 2),
        "msp_fee": round(msp_fee_sum, 2),
        "wc_fee": round(wc_fee_sum, 2),
        "cc_fee": round(cc_fee_sum, 2),
        "bt_fee": round(bt_fee_sum, 2),
        "commissions": round(commissions_sum, 2),
        "payroll_tax": round(payroll_tax, 2),
        "other_work": round(other_work_sum, 2),
        "profit": round(profit, 2),
        "client_breakdown": client_breakdown
    })


@router.post("/api/weekly-client-data")
async def get_weekly_client_data(payload: ProfitPayload):
    if not payload.client_name:
        raise HTTPException(status_code=400, detail="Client name is required.")

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
                c.sales_executive_id,
                c.won_date,
                u.last_name AS sales_executive_last_name,
                c.payment_type,
                c.billing_type_id,
                se.bill_rate,
                se.rate AS pay_rate,
                v.service_charge AS venue_service_charge,
                m.rate AS msp_rate,
                wc.rate AS wc_rate,
                e.state AS event_state,
                t.client_worked,
                t.employee_worked,
                s.start AS shift_start,
                s.end AS shift_end,
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
            LEFT JOIN user u ON c.sales_executive_id = u.id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND c.name = :client_name
              AND (
                  (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
              )
            """
        )

        params = {
            "start_date": payload.start_date, 
            "end_date": payload.end_date,
            "client_name": payload.client_name
        }

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)
            
            try:
                asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
                asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
                asp_rules = [dict(r) for r in asp_rules_raw]
            except Exception:
                asp_rules = []

    finally:
        engine.dispose()

    if df.empty:
        return JSONResponse({"weeks": []})

    numeric_cols = [
        "client_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    # Re-use the complex calculation logic from above (duplicated here for scope simplicity)
    def process_row(row):
        use_sheet = str(row.get("use_sheet") or "").upper()
        c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
        e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
        uses_both_sheets = (use_sheet == "")
        if uses_both_sheets:
            bill_seconds = c_sec
            pay_seconds = e_sec
        elif use_sheet == "EMPLOYEE":
            bill_seconds = e_sec
            pay_seconds = e_sec
        else:
            bill_seconds = c_sec
            pay_seconds = c_sec
        c_hours = bill_seconds / 3600.0
        e_hours = pay_seconds / 3600.0
        shift_start_raw = row.get("shift_start")
        shift_end_raw   = row.get("shift_end")
        if pd.notna(shift_start_raw) and pd.notna(shift_end_raw):
            shift_dur_hours = (pd.to_datetime(shift_end_raw) - pd.to_datetime(shift_start_raw)).total_seconds() / 3600.0
            meal_break_deduction = 0.5 if shift_dur_hours > 5.0 else 0.0
            shift_work_hours = shift_dur_hours - meal_break_deduction
            shift_senthome_min_hours = min(shift_work_hours / 2.0, 4.0)
            shift_min_bill_hours = 4.0 if shift_dur_hours >= 4.0 else 2.0
        else:
            shift_work_hours = 4.0
            shift_senthome_min_hours = 2.0
            shift_min_bill_hours = 4.0
        e_worked_raw = str(row.get("employee_worked") or "").upper()
        c_min = row.get("client_min_bill")
        e_min = row.get("employee_min_pay")
        state = str(row["event_state"]).upper() if row["event_state"] else ""
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
        late_hours = e_late_hours if use_sheet == "EMPLOYEE" else c_late_hours
        if pd.notna(c_min) and float(c_min) > 0:
            c_bill_reg = shift_min_bill_hours
            if late_hours > 0 and c_hours < c_bill_reg:
                c_bill_reg -= late_hours
            elif c_hours > c_bill_reg:
                c_bill_reg = c_hours
            c_bill_reg = max(c_bill_reg, 2.0)
            c_bill_reg = min(c_bill_reg, shift_min_bill_hours)
        else:
            c_bill_reg = c_hours
        c_non_worked = 0.0
        e_worked = e_worked_raw
        if e_worked in ("SENTHOME", "CANCELLED"):
            c_non_worked = max(c_bill_reg - c_hours, 0.0)
        c_worked_hours = c_bill_reg - c_non_worked
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
        is_senthome = e_worked in ("SENTHOME",)
        if is_senthome or (pd.notna(e_min) and float(e_min) > 0):
            e_pay_reg_floor = shift_senthome_min_hours
            e_pay_reg = e_pay_reg_floor
            if late_hours > 0 and e_hours < e_pay_reg:
                e_pay_reg -= late_hours
            elif e_hours > e_pay_reg:
                e_pay_reg = e_hours
            e_pay_reg = max(e_pay_reg, 2.0)
            e_pay_reg = min(e_pay_reg, e_pay_reg_floor)
        else:
            e_pay_reg = e_hours
        e_non_worked = 0.0
        if e_worked in ("SENTHOME", "CANCELLED"):
            e_non_worked = max(e_pay_reg - e_hours, 0.0)
        e_worked_hours = e_pay_reg - e_non_worked
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
        c_worked = str(row.get("client_worked") or "").upper()
        both_worked = (c_worked in ("WORKED", "SENTHOME") and e_worked in ("WORKED", "SENTHOME"))
        additional_pay = 0.0
        if both_worked and "date" in row and pd.notna(row["date"]):
            row_date = pd.to_datetime(row["date"]).date()
            for rule in asp_rules:
                r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
                r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
                if ((r_start is None) or (r_start <= row_date)) and ((r_end is None) or (r_end >= row_date)):
                    additional_pay += float(rule["rate"])
        bonus_pay = float(row.get("bonus") or 0.0) if both_worked else 0.0
        if c_worked not in ("WORKED", "SENTHOME"):
            service_amt_c = meal_amt_c = c_tips = c_parking = c_travel = 0.0
        else:
            c_tips = float(row.get("client_tips", 0))
            c_parking = float(row.get("client_parking", 0))
            c_travel = float(row.get("client_travel", 0))
        if e_worked not in ("WORKED", "SENTHOME"):
            service_amt_e = meal_amt_e = additional_pay = bonus_pay = e_tips = e_parking = e_travel = 0.0
        else:
            e_tips = float(row.get("employee_tips", 0))
            e_parking = float(row.get("employee_parking", 0))
            e_travel = float(row.get("employee_travel", 0))
        total_bill = calculate_total_bill(row)
        total_pay = reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + e_tips + e_parking + e_travel + additional_pay + bonus_pay
        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        # Credit card processing fee: 2.9% of total_bill for CC clients (payment_type=1)
        payment_type = row.get("payment_type")
        try:
            cc_fee = total_bill * 0.029 if (pd.notna(payment_type) and int(float(payment_type)) == 1) else 0.0
        except (ValueError, TypeError):
            cc_fee = 0.0

        # Billing type fee: 2.93% of total_bill for billing_type_id=7 clients
        billing_type = row.get("billing_type_id")
        try:
            bt_fee = total_bill * 0.0293 if (pd.notna(billing_type) and int(float(billing_type)) in (6, 7, 11)) else 0.0
        except (ValueError, TypeError):
            bt_fee = 0.0

        commissions = 0.0
        sales_executive_id = row.get("sales_executive_id")
        if pd.notna(sales_executive_id) and str(sales_executive_id).strip() != "":
            last_name = str(row.get("sales_executive_last_name") or "").strip()
            if last_name == "Inherited by":
                commissions = total_bill * 0.005
            else:
                won_date_raw = row.get("won_date")
                shift_date_raw = row.get("date")
                if pd.notna(won_date_raw) and pd.notna(shift_date_raw):
                    if pd.to_datetime(shift_date_raw) <= pd.to_datetime(won_date_raw) + pd.DateOffset(years=1):
                        commissions = total_bill * 0.03
                    else:
                        commissions = total_bill * 0.01
                else:
                    commissions = total_bill * 0.01

        return pd.Series({
            "date": row["date"],
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee,
            "cc_fee": cc_fee,
            "bt_fee": bt_fee,
            "commissions": commissions
        })

    calc_df = df.apply(process_row, axis=1)
    calc_df["date"] = pd.to_datetime(calc_df["date"])
    calc_df.set_index("date", inplace=True)
    
    weekly = calc_df.resample("W-MON", label="left", closed="left")[["total_bill", "total_pay", "msp_fee", "wc_fee", "cc_fee", "bt_fee", "commissions"]].sum()
    
    weeks_data = []
    for dt, r in weekly.iterrows():
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_cc = float(r["cc_fee"])
        c_bt = float(r["bt_fee"])
        c_commissions = float(r["commissions"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_cc - c_bt - c_commissions - c_tax
        c_profit_pct = (c_profit / c_bill * 100) if c_bill else 0.0
        
        weeks_data.append({
            "week_start": dt.strftime("%Y-%m-%d"),
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "cc_fee": round(c_cc, 2),
            "bt_fee": round(c_bt, 2),
            "commissions": round(c_commissions, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2),
            "profit_pct": round(c_profit_pct, 1),
        })

    return JSONResponse({"weeks": weeks_data})


@router.post("/api/position-breakdown")
async def get_position_breakdown(payload: ProfitPayload):
    if not payload.client_name:
        raise HTTPException(status_code=400, detail="Client name is required.")

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
                c.sales_executive_id,
                c.won_date,
                u.last_name AS sales_executive_last_name,
                c.payment_type,
                c.billing_type_id,
                se.bill_rate,
                se.rate AS pay_rate,
                v.service_charge AS venue_service_charge,
                m.rate AS msp_rate,
                wc.rate AS wc_rate,
                e.state AS event_state,
                t.client_worked,
                t.employee_worked,
                s.start AS shift_start,
                s.end AS shift_end,
                t.client_start,
                t.employee_start,
                p.description AS position_name,
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
            LEFT JOIN position p ON sp.position_id = p.position_id
            LEFT JOIN user u ON c.sales_executive_id = u.id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND c.name = :client_name
              AND (
                  (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
              )
            """
        )

        params = {
            "start_date": payload.start_date, 
            "end_date": payload.end_date,
            "client_name": payload.client_name
        }

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)
            
            try:
                asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
                asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
                asp_rules = [dict(r) for r in asp_rules_raw]
            except Exception:
                asp_rules = []

    finally:
        engine.dispose()

    if df.empty:
        return JSONResponse({"positions": []})

    numeric_cols = [
        "client_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    # Re-use the complex calculation logic
    def process_row(row):
        use_sheet = str(row.get("use_sheet") or "").upper()
        c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
        e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
        uses_both_sheets = (use_sheet == "")
        if uses_both_sheets:
            bill_seconds = c_sec
            pay_seconds = e_sec
        elif use_sheet == "EMPLOYEE":
            bill_seconds = e_sec
            pay_seconds = e_sec
        else:
            bill_seconds = c_sec
            pay_seconds = c_sec
        c_hours = bill_seconds / 3600.0
        e_hours = pay_seconds / 3600.0
        shift_start_raw = row.get("shift_start")
        shift_end_raw   = row.get("shift_end")
        if pd.notna(shift_start_raw) and pd.notna(shift_end_raw):
            shift_dur_hours = (pd.to_datetime(shift_end_raw) - pd.to_datetime(shift_start_raw)).total_seconds() / 3600.0
            meal_break_deduction = 0.5 if shift_dur_hours > 5.0 else 0.0
            shift_work_hours = shift_dur_hours - meal_break_deduction
            shift_senthome_min_hours = min(shift_work_hours / 2.0, 4.0)
            shift_min_bill_hours = 4.0 if shift_dur_hours >= 4.0 else 2.0
        else:
            shift_work_hours = 4.0
            shift_senthome_min_hours = 2.0
            shift_min_bill_hours = 4.0
        e_worked_raw = str(row.get("employee_worked") or "").upper()
        c_min = row.get("client_min_bill")
        e_min = row.get("employee_min_pay")
        state = str(row["event_state"]).upper() if row["event_state"] else ""
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
        late_hours = e_late_hours if use_sheet == "EMPLOYEE" else c_late_hours
        if pd.notna(c_min) and float(c_min) > 0:
            c_bill_reg = shift_min_bill_hours
            if late_hours > 0 and c_hours < c_bill_reg:
                c_bill_reg -= late_hours
            elif c_hours > c_bill_reg:
                c_bill_reg = c_hours
            c_bill_reg = max(c_bill_reg, 2.0)
            c_bill_reg = min(c_bill_reg, shift_min_bill_hours)
        else:
            c_bill_reg = c_hours
        c_non_worked = 0.0
        e_worked = e_worked_raw
        if e_worked in ("SENTHOME", "CANCELLED"):
            c_non_worked = max(c_bill_reg - c_hours, 0.0)
        c_worked_hours = c_bill_reg - c_non_worked
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
        is_senthome = e_worked in ("SENTHOME",)
        if is_senthome or (pd.notna(e_min) and float(e_min) > 0):
            e_pay_reg_floor = shift_senthome_min_hours
            e_pay_reg = e_pay_reg_floor
            if late_hours > 0 and e_hours < e_pay_reg:
                e_pay_reg -= late_hours
            elif e_hours > e_pay_reg:
                e_pay_reg = e_hours
            e_pay_reg = max(e_pay_reg, 2.0)
            e_pay_reg = min(e_pay_reg, e_pay_reg_floor)
        else:
            e_pay_reg = e_hours
        e_non_worked = 0.0
        if e_worked in ("SENTHOME", "CANCELLED"):
            e_non_worked = max(e_pay_reg - e_hours, 0.0)
        e_worked_hours = e_pay_reg - e_non_worked
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
        c_worked = str(row.get("client_worked") or "").upper()
        both_worked = (c_worked in ("WORKED", "SENTHOME") and e_worked in ("WORKED", "SENTHOME"))
        additional_pay = 0.0
        if both_worked and "date" in row and pd.notna(row["date"]):
            row_date = pd.to_datetime(row["date"]).date()
            for rule in asp_rules:
                r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
                r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
                if ((r_start is None) or (r_start <= row_date)) and ((r_end is None) or (r_end >= row_date)):
                    additional_pay += float(rule["rate"])
        bonus_pay = float(row.get("bonus") or 0.0) if both_worked else 0.0
        if c_worked not in ("WORKED", "SENTHOME"):
            service_amt_c = meal_amt_c = c_tips = c_parking = c_travel = 0.0
        else:
            c_tips = float(row.get("client_tips", 0))
            c_parking = float(row.get("client_parking", 0))
            c_travel = float(row.get("client_travel", 0))
        if e_worked not in ("WORKED", "SENTHOME"):
            service_amt_e = meal_amt_e = additional_pay = bonus_pay = e_tips = e_parking = e_travel = 0.0
        else:
            e_tips = float(row.get("employee_tips", 0))
            e_parking = float(row.get("employee_parking", 0))
            e_travel = float(row.get("employee_travel", 0))
        total_bill = calculate_total_bill(row)
        total_pay = reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + e_tips + e_parking + e_travel + additional_pay + bonus_pay
        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        # Credit card processing fee: 2.9% of total_bill for CC clients (payment_type=1)
        payment_type = row.get("payment_type")
        try:
            cc_fee = total_bill * 0.029 if (pd.notna(payment_type) and int(float(payment_type)) == 1) else 0.0
        except (ValueError, TypeError):
            cc_fee = 0.0

        # Billing type fee: 2.93% of total_bill for billing_type_id=7 clients
        billing_type = row.get("billing_type_id")
        try:
            bt_fee = total_bill * 0.0293 if (pd.notna(billing_type) and int(float(billing_type)) in (6, 7, 11)) else 0.0
        except (ValueError, TypeError):
            bt_fee = 0.0

        commissions = 0.0
        sales_executive_id = row.get("sales_executive_id")
        if pd.notna(sales_executive_id) and str(sales_executive_id).strip() != "":
            last_name = str(row.get("sales_executive_last_name") or "").strip()
            if last_name == "Inherited by":
                commissions = total_bill * 0.005
            else:
                won_date_raw = row.get("won_date")
                shift_date_raw = row.get("date")
                if pd.notna(won_date_raw) and pd.notna(shift_date_raw):
                    if pd.to_datetime(shift_date_raw) <= pd.to_datetime(won_date_raw) + pd.DateOffset(years=1):
                        commissions = total_bill * 0.03
                    else:
                        commissions = total_bill * 0.01
                else:
                    commissions = total_bill * 0.01

        return pd.Series({
            "position_name": row.get("position_name", "Unknown"),
            "bill_rate": bill_rate,
            "pay_rate": pay_rate,
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee,
            "cc_fee": cc_fee,
            "bt_fee": bt_fee,
            "commissions": commissions
        })

    calc_df = df.apply(process_row, axis=1)
    calc_df["position_name"] = calc_df["position_name"].fillna("Unknown")
    
    pos_group = calc_df.groupby(["position_name", "bill_rate", "pay_rate"])[["total_bill", "total_pay", "msp_fee", "wc_fee", "cc_fee", "bt_fee", "commissions"]].sum().reset_index()
    
    positions_data = []
    for _, r in pos_group.iterrows():
        c_bill_rate = float(r["bill_rate"])
        c_pay_rate = float(r["pay_rate"])
        c_markup = ((c_bill_rate - c_pay_rate) / c_pay_rate * 100) if c_pay_rate > 0 else 0.0

        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_cc = float(r["cc_fee"])
        c_bt = float(r["bt_fee"])
        c_commissions = float(r["commissions"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_cc - c_bt - c_commissions - c_tax
        c_profit_pct = (c_profit / c_bill * 100) if c_bill else 0.0
        
        positions_data.append({
            "position_name": r["position_name"],
            "bill_rate": round(c_bill_rate, 2),
            "pay_rate": round(c_pay_rate, 2),
            "markup_pct": round(c_markup, 1),
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "cc_fee": round(c_cc, 2),
            "bt_fee": round(c_bt, 2),
            "commissions": round(c_commissions, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2),
            "profit_pct": round(c_profit_pct, 1),
        })

    positions_data.sort(key=lambda x: x["total_bill"], reverse=True)
    return JSONResponse({"positions": positions_data})


# ── ACCOUNT TRACKING SYSTEM (SQLITE PERSISTENCE) ──────────────────

import sqlite3

DATA_DIR_TRACK = _resolve_data_dir()
os.makedirs(DATA_DIR_TRACK, exist_ok=True)
DB_PATH_TRACK = DATA_DIR_TRACK / "profit_tracker.db"


def _get_db_conn():
    conn = sqlite3.connect(DB_PATH_TRACK)
    conn.row_factory = sqlite3.Row
    return conn


def init_tracking_db():
    conn = _get_db_conn()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS client_profile (
            client_name TEXT PRIMARY KEY,
            owner TEXT,
            stuck_reason TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS client_next_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT,
            action_text TEXT,
            completed INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_at DATETIME
        )
    ''')
    conn.commit()
    conn.close()


# Self-initialize database
init_tracking_db()


@router.get("/api/account-tracking")
async def get_account_tracking():
    init_tracking_db()
    conn = _get_db_conn()
    try:
        cursor = conn.cursor()
        
        # Get profiles
        cursor.execute("SELECT client_name, owner, stuck_reason FROM client_profile")
        profiles = {
            row["client_name"]: {
                "owner": row["owner"],
                "stuck_reason": row["stuck_reason"],
                "active_next_action": None
            }
            for row in cursor.fetchall()
        }
        
        # Get active (incomplete) next actions
        cursor.execute("SELECT id, client_name, action_text, completed FROM client_next_actions WHERE completed = 0")
        for row in cursor.fetchall():
            c_name = row["client_name"]
            if c_name not in profiles:
                profiles[c_name] = {"owner": None, "stuck_reason": None, "active_next_action": None}
            profiles[c_name]["active_next_action"] = {
                "id": row["id"],
                "action_text": row["action_text"],
                "completed": row["completed"]
            }
            
        return JSONResponse(profiles)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()


class SetOwnerPayload(BaseModel):
    client_name: str
    owner: Optional[str] = None


@router.post("/api/account-tracking/owner")
async def set_client_owner(payload: SetOwnerPayload):
    conn = _get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO client_profile (client_name, owner) 
            VALUES (?, ?) 
            ON CONFLICT(client_name) DO UPDATE SET owner = excluded.owner, updated_at = CURRENT_TIMESTAMP
        ''', (payload.client_name, payload.owner))
        conn.commit()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()


class SetStuckReasonPayload(BaseModel):
    client_name: str
    stuck_reason: Optional[str] = None


@router.post("/api/account-tracking/stuck-reason")
async def set_client_stuck_reason(payload: SetStuckReasonPayload):
    conn = _get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO client_profile (client_name, stuck_reason) 
            VALUES (?, ?) 
            ON CONFLICT(client_name) DO UPDATE SET stuck_reason = excluded.stuck_reason, updated_at = CURRENT_TIMESTAMP
        ''', (payload.client_name, payload.stuck_reason))
        conn.commit()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()


class SetNextActionPayload(BaseModel):
    client_name: str
    action_text: Optional[str] = None


@router.post("/api/account-tracking/next-action")
async def set_client_next_action(payload: SetNextActionPayload):
    if not payload.action_text or not payload.action_text.strip():
        raise HTTPException(status_code=400, detail="Action text cannot be empty.")
        
    conn = _get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM client_next_actions WHERE client_name = ? AND completed = 0", (payload.client_name,))
        row = cursor.fetchone()
        
        if row:
            cursor.execute("UPDATE client_next_actions SET action_text = ? WHERE id = ?", (payload.action_text, row["id"]))
            action_id = row["id"]
        else:
            cursor.execute("INSERT INTO client_next_actions (client_name, action_text, completed) VALUES (?, ?, 0)", (payload.client_name, payload.action_text))
            action_id = cursor.lastrowid
            
        conn.commit()
        return JSONResponse({"status": "ok", "id": action_id})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()


class CompleteNextActionPayload(BaseModel):
    action_id: int


@router.post("/api/account-tracking/next-action/complete")
async def complete_client_next_action(payload: CompleteNextActionPayload):
    conn = _get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE client_next_actions SET completed = 1, completed_at = CURRENT_TIMESTAMP WHERE id = ?", (payload.action_id,))
        conn.commit()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        conn.close()

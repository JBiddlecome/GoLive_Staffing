import os
import pandas as pd
import numpy as np
from fastapi import APIRouter, HTTPException, Request, File, UploadFile
from pathlib import Path
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Dict, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL

def _db_url_from_env() -> URL:
    env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)
        
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
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


router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Resolve path dynamically to support both local Windows and live Render server
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXCEL_PATH = os.path.join(BASE_DIR, "shifts_report_april_may_2026.xlsx")

LOCAL_FALLBACK = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\shifts_report_april_may_2026.xlsx"
if not os.path.exists(EXCEL_PATH) and os.path.exists(LOCAL_FALLBACK):
    EXCEL_PATH = LOCAL_FALLBACK

import shutil
import uuid

def read_excel_robust(file_path: str) -> pd.DataFrame:
    """Reads Excel file safely even if locked by another program on Windows."""
    temp_dir = os.path.join(os.path.dirname(file_path), "tmp")
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = os.path.join(temp_dir, f"temp_{uuid.uuid4().hex}_{os.path.basename(file_path)}")
    try:
        shutil.copy2(file_path, temp_path)
        return pd.read_excel(temp_path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

def parse_uploaded_file(file_path: str) -> Dict[int, Dict[str, float]]:
    rates = {}
    if file_path.lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)
        
    normalized_cols = {c: str(c).strip().lower().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=normalized_cols)
    
    reqs = ['venue_position_id', 'new_pay', 'new_bill']
    missing = [r for r in reqs if r not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in spreadsheet: {', '.join(missing)}")
        
    df = df.dropna(subset=['venue_position_id'])
    for _, row in df.iterrows():
        try:
            vpid = int(float(row['venue_position_id']))
            new_pay = float(row['new_pay'])
            new_bill = float(row['new_bill'])
            rates[vpid] = {
                'new_pay': new_pay,
                'new_bill': new_bill
            }
        except (ValueError, TypeError):
            continue
    return rates

def load_saved_spreadsheet_rates() -> Dict[int, Dict[str, float]]:
    std_dir = os.path.join(BASE_DIR, "tmp", "pay_rate_reduction_calculator")
    os.makedirs(std_dir, exist_ok=True)
    std_csv_path = os.path.join(std_dir, "uploaded_rates.csv")
    fallback_path = os.path.join(BASE_DIR, "apps", "pay_rate_reduction_calculator", "Markup_Analysis_Option_A.csv")
    
    if not os.path.exists(std_csv_path) and os.path.exists(fallback_path):
        try:
            rates = parse_uploaded_file(fallback_path)
            df_std = pd.DataFrame([
                {"venue_position_id": k, "new_pay": v["new_pay"], "new_bill": v["new_bill"]}
                for k, v in rates.items()
            ])
            df_std.to_csv(std_csv_path, index=False)
        except Exception as e:
            print("Failed to seed uploaded_rates.csv from Markup_Analysis_Option_A.csv:", e)
            
    if os.path.exists(std_csv_path):
        try:
            df = pd.read_csv(std_csv_path)
            rates = {}
            for _, row in df.iterrows():
                try:
                    vpid = int(float(row['venue_position_id']))
                    rates[vpid] = {
                        'new_pay': float(row['new_pay']),
                        'new_bill': float(row['new_bill'])
                    }
                except (ValueError, TypeError):
                    continue
            return rates
        except Exception as e:
            print(f"Error reading std_csv_path: {e}")
            
    return {}

class RecalculateRequest(BaseModel):
    custom_rates: Dict[str, Optional[float]]

@router.get("", response_class=HTMLResponse)
async def pay_rate_reduction_calculator_page(request: Request):
    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(
            status_code=404,
            detail=f"Shift report Excel file not found at: {EXCEL_PATH}. Please generate it first."
        )
        
    try:
        df = read_excel_robust(EXCEL_PATH)
        
        # Replace NaNs in crucial columns
        df['Original Rate'] = df['Original Rate'].fillna(0.0).astype(float)
        df['Hours Worked'] = df['Hours Worked'].fillna(0.0).astype(float)
        df['Minimum Wage'] = df['Minimum Wage'].fillna(0.0).astype(float)
        df['Less $2 Rate'] = df['Less $2 Rate'].fillna(df['Original Rate']).astype(float)
        df['Rate Lock'] = df['Rate Lock'].fillna('No').astype(str).str.strip()
        df['County'] = df['County'].fillna('Unknown County').astype(str).str.strip()
        df['Position Title'] = df['Position Title'].fillna('Unknown Position').astype(str).str.strip()
        
        # 1. Calculate Baselines
        total_original_paid = float((df['Hours Worked'] * df['Original Rate']).sum())
        total_less_2_paid = float((df['Hours Worked'] * df['Less $2 Rate']).sum())
        savings_less_2 = total_original_paid - total_less_2_paid
        savings_pct_less_2 = (savings_less_2 / total_original_paid * 100) if total_original_paid > 0 else 0.0
        
        # 2. Group by County and Position Title for inputs
        # We want to exclude rows where County or Position is missing or default Unknown
        valid_df = df[
            (df['County'] != 'Unknown County') & 
            (df['Position Title'] != 'Unknown Position')
        ]
        
        grouped = valid_df.groupby(['County', 'Position Title']).agg(
            shifts_count=('Shift Employee ID', 'count'),
            avg_orig_rate=('Original Rate', 'mean'),
            min_wage=('Minimum Wage', 'min'), # Min to get the lowest min wage floor for the UI input
            max_min_wage=('Minimum Wage', 'max'), # Max to check if there is a range of min wages
            locked_count=('Rate Lock', lambda x: (x.str.lower() == 'yes').sum())
        ).reset_index()
        
        # Convert to dictionary sorted by County
        counties_data = {}
        for _, row in grouped.iterrows():
            county = row['County']
            if county not in counties_data:
                counties_data[county] = []
                
            counties_data[county].append({
                "position": row['Position Title'],
                "shifts": int(row['shifts_count']),
                "avg_orig_rate": round(float(row['avg_orig_rate']), 2),
                "min_wage": round(float(row['min_wage']), 2),
                "max_min_wage": round(float(row['max_min_wage']), 2),
                "locked_shifts": int(row['locked_count'])
            })
            
        # Sort positions within each county by shift count descending
        for county in counties_data:
            counties_data[county].sort(key=lambda x: x['shifts'], reverse=True)
            
        # Sort counties by total shifts descending
        sorted_counties = sorted(
            counties_data.items(), 
            key=lambda x: sum(p['shifts'] for p in x[1]), 
            reverse=True
        )
        
        msps_list = []
        billing_types_list = []
        try:
            engine_inst = _engine()
            with engine_inst.begin() as connection:
                msps_rows = connection.execute(text("SELECT id, name FROM msp ORDER BY name")).mappings().fetchall()
                msps_list = [dict(r) for r in msps_rows]
                bt_rows = connection.execute(text("SELECT id, name FROM billing_type ORDER BY name")).mappings().fetchall()
                billing_types_list = [dict(r) for r in bt_rows]
            engine_inst.dispose()
        except Exception as db_err:
            print("Warning: Could not load MSP/billing_type list from DB for calculator:", db_err)

        return templates.TemplateResponse(
            "apps/pay_rate_reduction_calculator.html",
            {
                "request": request,
                "total_original_paid": total_original_paid,
                "total_less_2_paid": total_less_2_paid,
                "savings_less_2": savings_less_2,
                "savings_pct_less_2": savings_pct_less_2,
                "counties": sorted_counties,
                "total_shifts": len(df),
                "msps": msps_list,
                "billing_types": billing_types_list
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shift report: {str(e)}")

@router.post("/calculate")
async def calculate_custom_rates(payload: RecalculateRequest):
    if not os.path.exists(EXCEL_PATH):
        return JSONResponse(
            status_code=404,
            content={"message": "Shift report Excel file not found."}
        )
        
    try:
        df = read_excel_robust(EXCEL_PATH)
        
        df['Original Rate'] = df['Original Rate'].fillna(0.0).astype(float)
        df['Hours Worked'] = df['Hours Worked'].fillna(0.0).astype(float)
        df['Minimum Wage'] = df['Minimum Wage'].fillna(0.0).astype(float)
        df['Rate Lock'] = df['Rate Lock'].fillna('No').astype(str).str.strip()
        df['County'] = df['County'].fillna('Unknown County').astype(str).str.strip()
        df['Position Title'] = df['Position Title'].fillna('Unknown Position').astype(str).str.strip()
        
        custom_rates_map = payload.custom_rates
        
        total_original_paid = 0.0
        total_custom_paid = 0.0
        
        # Process rows in a high-speed loop
        for _, row in df.iterrows():
            hours = float(row['Hours Worked'])
            orig_rate = float(row['Original Rate'])
            min_wage = float(row['Minimum Wage'])
            lock = str(row['Rate Lock']).lower() == 'yes'
            county = str(row['County'])
            pos = str(row['Position Title'])
            
            key = f"{county}|{pos}"
            
            total_original_paid += hours * orig_rate
            
            # Apply Custom rate logic
            if key in custom_rates_map and custom_rates_map[key] is not None:
                custom_rate = custom_rates_map[key]
                if lock:
                    # Locked rows CANNOT have their rates changed
                    final_rate = orig_rate
                else:
                    # No rate can fall below Minimum Wage
                    final_rate = max(custom_rate, min_wage)
            else:
                final_rate = orig_rate
                
            total_custom_paid += hours * final_rate
            
        savings = total_original_paid - total_custom_paid
        savings_pct = (savings / total_original_paid * 100) if total_original_paid > 0 else 0.0
        
        return {
            "total_original_paid": total_original_paid,
            "total_custom_paid": total_custom_paid,
            "savings": savings,
            "savings_pct": savings_pct
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"message": f"Error running calculations: {str(e)}"}
        )



class CompassCalculateRequest(BaseModel):
    start_date: str
    end_date: str
    reduction_amount: float = 2.0
    markup_percent: float = 77.0
    msp_filter: str = "2"
    client_reductions: Optional[Dict[str, float]] = None
    use_spreadsheet: bool = False

# Action Capital billing_type IDs (combined into a single option)
ACTION_CAPITAL_BT_IDS = {2, 3, 6, 7, 11}

class BillingTypeCalculateRequest(BaseModel):
    start_date: str
    end_date: str
    reduction_amount: float = 2.0
    markup_percent: float = 77.0
    billing_type_filter: str = "action_capital"
    client_reductions: Optional[Dict[str, float]] = None
    use_spreadsheet: bool = False

@router.post("/upload-spreadsheet")
async def upload_spreadsheet(file: UploadFile = File(...)):
    contents = await file.read()
    temp_dir = os.path.join(BASE_DIR, "tmp", "pay_rate_reduction_calculator")
    os.makedirs(temp_dir, exist_ok=True)
    
    filename = file.filename or "upload.csv"
    file_suffix = Path(filename).suffix or ".csv"
    temp_path = os.path.join(temp_dir, f"temp_upload_{uuid.uuid4().hex}{file_suffix}")
    
    try:
        with open(temp_path, "wb") as f:
            f.write(contents)
            
        rates = parse_uploaded_file(temp_path)
        if not rates:
            raise HTTPException(status_code=400, detail="No valid rates found in spreadsheet.")
            
        std_csv_path = os.path.join(temp_dir, "uploaded_rates.csv")
        df_std = pd.DataFrame([
            {"venue_position_id": k, "new_pay": v["new_pay"], "new_bill": v["new_bill"]}
            for k, v in rates.items()
        ])
        df_std.to_csv(std_csv_path, index=False)
        
        return {
            "success": True,
            "message": f"Successfully processed {len(rates)} venue position rates.",
            "count": len(rates)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

@router.post("/compass/calculate")
async def calculate_compass_rates(payload: CompassCalculateRequest):
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

        msp_clause = ""
        params = {"start_date": payload.start_date, "end_date": payload.end_date}

        if payload.msp_filter == "none":
            msp_clause = "AND c.msp_id IS NULL"
        elif payload.msp_filter == "all":
            msp_clause = ""
        else:
            try:
                msp_id_val = int(payload.msp_filter)
                msp_clause = "AND c.msp_id = :msp_id"
                params["msp_id"] = msp_id_val
            except ValueError:
                # Fallback to Compass if parsing fails
                msp_clause = "AND c.msp_id = 2"

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
                COALESCE(mwra.rate, 0.0) AS min_wage,
                vp.venue_position_id,
                COALESCE(pos.description, 'Unknown Position') AS position_name,
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
            JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
            LEFT JOIN position pos ON sp.position_id = pos.position_id
            LEFT JOIN user u ON c.sales_executive_id = u.id
            LEFT JOIN min_wage_rate_amount mwra ON v.min_wage_id = mwra.min_wage_id
                AND (mwra.start_date IS NULL OR mwra.start_date <= '2026-07-01')
                AND (mwra.end_date IS NULL OR mwra.end_date >= '2026-07-01')
            WHERE e.date >= :start_date AND e.date <= :end_date
              {msp_clause}
              AND (
                  (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
              )
            """
        )



        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)
            
            try:
                asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
                asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
                asp_rules = [dict(r) for r in asp_rules_raw]
            except Exception:
                asp_rules = []

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"message": f"Database query error: {str(e)}"}
        )
    finally:
        engine.dispose()

    if df.empty:
        return {
            "baseline": {
                "total_bill": 0.0,
                "gross_pay": 0.0,
                "msp_fee": 0.0,
                "wc_fee": 0.0,
                "cc_fee": 0.0,
                "bt_fee": 0.0,
                "commissions": 0.0,
                "payroll_tax": 0.0,
                "total_fees": 0.0,
                "profit": 0.0
            },
            "simulated": {
                "total_bill": 0.0,
                "gross_pay": 0.0,
                "msp_fee": 0.0,
                "wc_fee": 0.0,
                "cc_fee": 0.0,
                "bt_fee": 0.0,
                "commissions": 0.0,
                "payroll_tax": 0.0,
                "total_fees": 0.0,
                "profit": 0.0
            },
            "client_breakdown": []
        }

    numeric_cols = [
        "client_seconds", "employee_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate", "min_wage"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    def calculate_financials(row, simulate=False):
        use_sheet = str(row.get("use_sheet") or "").upper()
        c_sec = float(row["client_seconds"])
        e_sec = float(row["employee_seconds"])

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

        # Client Billing hours
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

        # Employee Pay hours
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

        orig_pay_rate = float(row["pay_rate"])
        orig_bill_rate = float(row["bill_rate"])
        min_wage = float(row["min_wage"])

        if simulate:
            if payload.use_spreadsheet:
                vpid_val = row.get("venue_position_id")
                matched = False
                if pd.notna(vpid_val):
                    try:
                        vpid = int(float(vpid_val))
                        if vpid in spreadsheet_rates:
                            pay_rate = spreadsheet_rates[vpid]["new_pay"]
                            bill_rate = spreadsheet_rates[vpid]["new_bill"]
                            pay_rate = max(pay_rate, min_wage)
                            matched = True
                    except (ValueError, TypeError):
                        pass
                if not matched:
                    pay_rate = orig_pay_rate
                    bill_rate = orig_bill_rate
            else:
                client_name = row["client_name"]
                reduction = payload.reduction_amount
                if payload.client_reductions and client_name in payload.client_reductions:
                    reduction = payload.client_reductions[client_name]
                pay_rate = max(orig_pay_rate - reduction, min_wage)
                bill_rate = pay_rate * (1.0 + payload.markup_percent / 100.0)
        else:
            pay_rate = orig_pay_rate
            bill_rate = orig_bill_rate

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
                start_ok = (r_start is None) or (r_start <= row_date)
                end_ok = (r_end is None) or (r_end >= row_date)
                if start_ok and end_ok:
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

        total_bill = reg_bill + ot_bill + dt_bill + non_worked_bill + service_amt_c + meal_amt_c + c_tips + c_parking + c_travel
        total_pay = reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + e_tips + e_parking + e_travel + additional_pay + bonus_pay

        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        payment_type = row.get("payment_type")
        cc_fee = total_bill * 0.029 if (pd.notna(payment_type) and int(float(payment_type)) == 1) else 0.0

        billing_type = row.get("billing_type_id")
        has_bt_fee = bool(pd.notna(billing_type) and int(float(billing_type)) in (6, 7, 11))
        bt_fee = total_bill * 0.0293 if has_bt_fee else 0.0

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
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee,
            "cc_fee": cc_fee,
            "bt_fee": bt_fee,
            "commissions": commissions
        })

    spreadsheet_rates = {}
    if payload.use_spreadsheet:
        spreadsheet_rates = load_saved_spreadsheet_rates()

    # Run baseline and simulated calculations
    base_df = df.apply(lambda r: calculate_financials(r, False), axis=1)
    sim_df = df.apply(lambda r: calculate_financials(r, True), axis=1)

    # 1. Baseline Totals
    b_bill = float(base_df["total_bill"].sum())
    b_pay = float(base_df["total_pay"].sum())
    b_msp = float(base_df["msp_fee"].sum())
    b_wc = float(base_df["wc_fee"].sum())
    b_cc = float(base_df["cc_fee"].sum())
    b_bt = float(base_df["bt_fee"].sum())
    b_comm = float(base_df["commissions"].sum())
    b_tax = b_pay * 0.10
    b_tot_fees = b_msp + b_wc + b_cc + b_bt + b_comm + b_tax
    b_profit = b_bill - b_pay - b_tot_fees

    # 2. Simulated Totals
    s_bill = float(sim_df["total_bill"].sum())
    s_pay = float(sim_df["total_pay"].sum())
    s_msp = float(sim_df["msp_fee"].sum())
    s_wc = float(sim_df["wc_fee"].sum())
    s_cc = float(sim_df["cc_fee"].sum())
    s_bt = float(sim_df["bt_fee"].sum())
    s_comm = float(sim_df["commissions"].sum())
    s_tax = s_pay * 0.10
    s_tot_fees = s_msp + s_wc + s_cc + s_bt + s_comm + s_tax
    s_profit = s_bill - s_pay - s_tot_fees

    # 3. Client Breakdown Aggregations
    base_client = base_df.groupby("client_name").sum().reset_index()
    sim_client = sim_df.groupby("client_name").sum().reset_index()

    client_map = {}
    for _, r in base_client.iterrows():
        c_name = r["client_name"]
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_fees = float(r["msp_fee"] + r["wc_fee"] + r["cc_fee"] + r["bt_fee"] + r["commissions"] + (r["total_pay"] * 0.10))
        c_prof = c_bill - c_pay - c_fees
        client_map[c_name] = {
            "client_name": c_name,
            "orig_bill": round(c_bill, 2),
            "orig_pay": round(c_pay, 2),
            "orig_profit": round(c_prof, 2),
            "sim_bill": 0.0,
            "sim_pay": 0.0,
            "sim_profit": 0.0,
            "profit_change": 0.0
        }

    for _, r in sim_client.iterrows():
        c_name = r["client_name"]
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_fees = float(r["msp_fee"] + r["wc_fee"] + r["cc_fee"] + r["bt_fee"] + r["commissions"] + (r["total_pay"] * 0.10))
        c_prof = c_bill - c_pay - c_fees
        if c_name in client_map:
            client_map[c_name]["sim_bill"] = round(c_bill, 2)
            client_map[c_name]["sim_pay"] = round(c_pay, 2)
            client_map[c_name]["sim_profit"] = round(c_prof, 2)
            client_map[c_name]["profit_change"] = round(c_prof - client_map[c_name]["orig_profit"], 2)

    # Per-client, per-position drill-down
    client_positions = {}
    if "position_name" in df.columns:
        pos_group = df.groupby(["client_name", "position_name"]).agg(
            avg_pay_rate=("pay_rate", "mean"),
            avg_bill_rate=("bill_rate", "mean"),
            shift_count=("pay_rate", "count")
        ).reset_index()
        for _, r in pos_group.iterrows():
            c_name = r["client_name"]
            pos_name = r["position_name"]
            avg_pay = round(float(r["avg_pay_rate"]), 2)
            avg_bill = round(float(r["avg_bill_rate"]), 2)
            sim_pay = round(max(avg_pay - payload.reduction_amount, 0.0), 2)
            sim_bill = round(sim_pay * (1.0 + payload.markup_percent / 100.0), 2)
            current_markup = round(((avg_bill / avg_pay) - 1.0) * 100.0, 1) if avg_pay > 0 else 0.0
            if c_name not in client_positions:
                client_positions[c_name] = []
            client_positions[c_name].append({
                "position_name": pos_name,
                "avg_pay_rate": avg_pay,
                "avg_bill_rate": avg_bill,
                "sim_pay_rate": sim_pay,
                "sim_bill_rate": sim_bill,
                "current_markup_pct": current_markup,
                "sim_markup_pct": round(payload.markup_percent, 1),
                "shift_count": int(r["shift_count"])
            })
        for c_name in client_positions:
            client_positions[c_name].sort(key=lambda x: x["shift_count"], reverse=True)

    breakdown_list = list(client_map.values())
    breakdown_list.sort(key=lambda x: x["orig_bill"], reverse=True)

    return {
        "markup_percent": payload.markup_percent,
        "baseline": {
            "total_bill": round(b_bill, 2),
            "gross_pay": round(b_pay, 2),
            "msp_fee": round(b_msp, 2),
            "wc_fee": round(b_wc, 2),
            "cc_fee": round(b_cc, 2),
            "bt_fee": round(b_bt, 2),
            "commissions": round(b_comm, 2),
            "payroll_tax": round(b_tax, 2),
            "total_fees": round(b_tot_fees, 2),
            "profit": round(b_profit, 2)
        },
        "simulated": {
            "total_bill": round(s_bill, 2),
            "gross_pay": round(s_pay, 2),
            "msp_fee": round(s_msp, 2),
            "wc_fee": round(s_wc, 2),
            "cc_fee": round(s_cc, 2),
            "bt_fee": round(s_bt, 2),
            "commissions": round(s_comm, 2),
            "payroll_tax": round(s_tax, 2),
            "total_fees": round(s_tot_fees, 2),
            "profit": round(s_profit, 2)
        },
        "client_breakdown": breakdown_list,
        "client_positions": client_positions
    }

@router.post("/billing-type/calculate")
async def calculate_billing_type_rates(payload: BillingTypeCalculateRequest):
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

        params = {"start_date": payload.start_date, "end_date": payload.end_date}

        # Build billing_type WHERE clause
        if payload.billing_type_filter == "all":
            bt_clause = ""
        elif payload.billing_type_filter == "action_capital":
            # Combines billing_type IDs 2, 3, 6, 7, 11
            bt_clause = f"AND c.billing_type_id IN ({','.join(str(i) for i in sorted(ACTION_CAPITAL_BT_IDS))})"
        else:
            try:
                bt_id = int(payload.billing_type_filter)
                bt_clause = "AND c.billing_type_id = :bt_id"
                params["bt_id"] = bt_id
            except ValueError:
                bt_clause = ""

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
                COALESCE(mwra.rate, 0.0) AS min_wage,
                vp.venue_position_id,
                COALESCE(pos.description, 'Unknown Position') AS position_name,
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
            JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
            LEFT JOIN position pos ON sp.position_id = pos.position_id
            LEFT JOIN user u ON c.sales_executive_id = u.id
            LEFT JOIN min_wage_rate_amount mwra ON v.min_wage_id = mwra.min_wage_id
                AND (mwra.start_date IS NULL OR mwra.start_date <= '2026-07-01')
                AND (mwra.end_date IS NULL OR mwra.end_date >= '2026-07-01')
            WHERE e.date >= :start_date AND e.date <= :end_date
              {bt_clause}
              AND (
                  (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
              )
            """
        )

        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params=params)

            try:
                asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
                asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
                asp_rules = [dict(r) for r in asp_rules_raw]
            except Exception:
                asp_rules = []

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"message": f"Database query error: {str(e)}"}
        )
    finally:
        engine.dispose()

    if df.empty:
        empty_summary = {
            "total_bill": 0.0, "gross_pay": 0.0, "msp_fee": 0.0, "wc_fee": 0.0,
            "cc_fee": 0.0, "bt_fee": 0.0, "commissions": 0.0, "payroll_tax": 0.0,
            "total_fees": 0.0, "profit": 0.0
        }
        return {"baseline": empty_summary, "simulated": empty_summary, "client_breakdown": []}

    numeric_cols = [
        "client_seconds", "employee_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge",
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate", "min_wage"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    def calculate_financials_bt(row, simulate=False):
        use_sheet = str(row.get("use_sheet") or "").upper()
        c_sec = float(row["client_seconds"])
        e_sec = float(row["employee_seconds"])

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

        orig_pay_rate = float(row["pay_rate"])
        orig_bill_rate = float(row["bill_rate"])
        min_wage = float(row["min_wage"])

        if simulate:
            if payload.use_spreadsheet:
                vpid_val = row.get("venue_position_id")
                matched = False
                if pd.notna(vpid_val):
                    try:
                        vpid = int(float(vpid_val))
                        if vpid in bt_spreadsheet_rates:
                            pay_rate = bt_spreadsheet_rates[vpid]["new_pay"]
                            bill_rate = bt_spreadsheet_rates[vpid]["new_bill"]
                            pay_rate = max(pay_rate, min_wage)
                            matched = True
                    except (ValueError, TypeError):
                        pass
                if not matched:
                    pay_rate = orig_pay_rate
                    bill_rate = orig_bill_rate
            else:
                client_name = row["client_name"]
                reduction = payload.reduction_amount
                if payload.client_reductions and client_name in payload.client_reductions:
                    reduction = payload.client_reductions[client_name]
                pay_rate = max(orig_pay_rate - reduction, min_wage)
                bill_rate = pay_rate * (1.0 + payload.markup_percent / 100.0)
        else:
            pay_rate = orig_pay_rate
            bill_rate = orig_bill_rate

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
                start_ok = (r_start is None) or (r_start <= row_date)
                end_ok = (r_end is None) or (r_end >= row_date)
                if start_ok and end_ok:
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

        total_bill = reg_bill + ot_bill + dt_bill + non_worked_bill + service_amt_c + meal_amt_c + c_tips + c_parking + c_travel
        total_pay = reg_pay + ot_pay + dt_pay + non_worked_pay + service_amt_e + meal_amt_e + e_tips + e_parking + e_travel + additional_pay + bonus_pay

        msp_rate = float(row.get("msp_rate", 0))
        wc_rate = float(row.get("wc_rate", 0))
        msp_fee = total_bill * msp_rate
        wc_fee = total_bill * wc_rate

        payment_type = row.get("payment_type")
        cc_fee = total_bill * 0.029 if (pd.notna(payment_type) and int(float(payment_type)) == 1) else 0.0

        billing_type = row.get("billing_type_id")
        has_bt_fee = bool(pd.notna(billing_type) and int(float(billing_type)) in (6, 7, 11))
        bt_fee = total_bill * 0.0293 if has_bt_fee else 0.0

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
            "total_bill": total_bill,
            "total_pay": total_pay,
            "msp_fee": msp_fee,
            "wc_fee": wc_fee,
            "cc_fee": cc_fee,
            "bt_fee": bt_fee,
            "commissions": commissions
        })

    bt_spreadsheet_rates = {}
    if payload.use_spreadsheet:
        bt_spreadsheet_rates = load_saved_spreadsheet_rates()

    base_df = df.apply(lambda r: calculate_financials_bt(r, False), axis=1)
    sim_df = df.apply(lambda r: calculate_financials_bt(r, True), axis=1)

    b_bill = float(base_df["total_bill"].sum())
    b_pay = float(base_df["total_pay"].sum())
    b_msp = float(base_df["msp_fee"].sum())
    b_wc = float(base_df["wc_fee"].sum())
    b_cc = float(base_df["cc_fee"].sum())
    b_bt = float(base_df["bt_fee"].sum())
    b_comm = float(base_df["commissions"].sum())
    b_tax = b_pay * 0.10
    b_tot_fees = b_msp + b_wc + b_cc + b_bt + b_comm + b_tax
    b_profit = b_bill - b_pay - b_tot_fees

    s_bill = float(sim_df["total_bill"].sum())
    s_pay = float(sim_df["total_pay"].sum())
    s_msp = float(sim_df["msp_fee"].sum())
    s_wc = float(sim_df["wc_fee"].sum())
    s_cc = float(sim_df["cc_fee"].sum())
    s_bt = float(sim_df["bt_fee"].sum())
    s_comm = float(sim_df["commissions"].sum())
    s_tax = s_pay * 0.10
    s_tot_fees = s_msp + s_wc + s_cc + s_bt + s_comm + s_tax
    s_profit = s_bill - s_pay - s_tot_fees

    base_client = base_df.groupby("client_name").sum().reset_index()
    sim_client = sim_df.groupby("client_name").sum().reset_index()

    client_map = {}
    for _, r in base_client.iterrows():
        c_name = r["client_name"]
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_fees = float(r["msp_fee"] + r["wc_fee"] + r["cc_fee"] + r["bt_fee"] + r["commissions"] + (r["total_pay"] * 0.10))
        c_prof = c_bill - c_pay - c_fees
        client_map[c_name] = {
            "client_name": c_name,
            "orig_bill": round(c_bill, 2),
            "orig_pay": round(c_pay, 2),
            "orig_profit": round(c_prof, 2),
            "sim_bill": 0.0,
            "sim_pay": 0.0,
            "sim_profit": 0.0,
            "profit_change": 0.0
        }

    for _, r in sim_client.iterrows():
        c_name = r["client_name"]
        c_bill = float(r["total_bill"])
        c_pay = float(r["total_pay"])
        c_fees = float(r["msp_fee"] + r["wc_fee"] + r["cc_fee"] + r["bt_fee"] + r["commissions"] + (r["total_pay"] * 0.10))
        c_prof = c_bill - c_pay - c_fees
        if c_name in client_map:
            client_map[c_name]["sim_bill"] = round(c_bill, 2)
            client_map[c_name]["sim_pay"] = round(c_pay, 2)
            client_map[c_name]["sim_profit"] = round(c_prof, 2)
            client_map[c_name]["profit_change"] = round(c_prof - client_map[c_name]["orig_profit"], 2)

    # Per-client, per-position drill-down
    client_positions = {}
    if "position_name" in df.columns:
        pos_group = df.groupby(["client_name", "position_name"]).agg(
            avg_pay_rate=("pay_rate", "mean"),
            avg_bill_rate=("bill_rate", "mean"),
            shift_count=("pay_rate", "count")
        ).reset_index()
        for _, r in pos_group.iterrows():
            c_name = r["client_name"]
            pos_name = r["position_name"]
            avg_pay = round(float(r["avg_pay_rate"]), 2)
            avg_bill = round(float(r["avg_bill_rate"]), 2)
            sim_pay = round(max(avg_pay - payload.reduction_amount, 0.0), 2)
            sim_bill = round(sim_pay * (1.0 + payload.markup_percent / 100.0), 2)
            current_markup = round(((avg_bill / avg_pay) - 1.0) * 100.0, 1) if avg_pay > 0 else 0.0
            if c_name not in client_positions:
                client_positions[c_name] = []
            client_positions[c_name].append({
                "position_name": pos_name,
                "avg_pay_rate": avg_pay,
                "avg_bill_rate": avg_bill,
                "sim_pay_rate": sim_pay,
                "sim_bill_rate": sim_bill,
                "current_markup_pct": current_markup,
                "sim_markup_pct": round(payload.markup_percent, 1),
                "shift_count": int(r["shift_count"])
            })
        for c_name in client_positions:
            client_positions[c_name].sort(key=lambda x: x["shift_count"], reverse=True)

    breakdown_list = list(client_map.values())
    breakdown_list.sort(key=lambda x: x["orig_bill"], reverse=True)

    return {
        "markup_percent": payload.markup_percent,
        "baseline": {
            "total_bill": round(b_bill, 2),
            "gross_pay": round(b_pay, 2),
            "msp_fee": round(b_msp, 2),
            "wc_fee": round(b_wc, 2),
            "cc_fee": round(b_cc, 2),
            "bt_fee": round(b_bt, 2),
            "commissions": round(b_comm, 2),
            "payroll_tax": round(b_tax, 2),
            "total_fees": round(b_tot_fees, 2),
            "profit": round(b_profit, 2)
        },
        "simulated": {
            "total_bill": round(s_bill, 2),
            "gross_pay": round(s_pay, 2),
            "msp_fee": round(s_msp, 2),
            "wc_fee": round(s_wc, 2),
            "cc_fee": round(s_cc, 2),
            "bt_fee": round(s_bt, 2),
            "commissions": round(s_comm, 2),
            "payroll_tax": round(s_tax, 2),
            "total_fees": round(s_tot_fees, 2),
            "profit": round(s_profit, 2)
        },
        "client_breakdown": breakdown_list,
        "client_positions": client_positions
    }


# === Rate Report ===
import json
import io
import csv
from fastapi.responses import StreamingResponse

def _resolve_rate_report_dir() -> Path:
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    return Path("data")

RATE_REPORT_FILE = _resolve_rate_report_dir() / "rate_report_adjustments.json"

CLIENT_STATUS_MAP = {1: "Active", 10: "Inactive 60", 11: "Inactive 180"}
RATE_REPORT_STATUSES = tuple(CLIENT_STATUS_MAP.keys())


def _load_rate_report_adjustments() -> dict:
    try:
        RATE_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        if RATE_REPORT_FILE.exists():
            with RATE_REPORT_FILE.open("r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_rate_report_adjustments(data: dict):
    RATE_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RATE_REPORT_FILE.open("w") as f:
        json.dump(data, f, indent=2)


class RateReportSaveRequest(BaseModel):
    adjustments: dict


@router.get("/rate-report/clients")
async def get_rate_report_clients():
    engine = _engine()
    try:
        sql = text("""
            SELECT
                c.client_id,
                c.name AS client_name,
                c.status AS client_status,
                c.msp_id,
                m.name AS msp_name,
                e.venue_id,
                v.name AS venue_name,
                vp.venue_position_id,
                pos.description AS position_name,
                AVG(se.rate) AS avg_pay_rate,
                AVG(se.bill_rate) AS avg_bill_rate,
                COUNT(*) AS shift_count,
                MAX(e.date) AS last_used,
                COALESCE((
                    SELECT mwra.rate
                    FROM min_wage_rate_amount mwra
                    WHERE mwra.min_wage_id = v.min_wage_id
                      AND (mwra.start_date IS NULL OR mwra.start_date <= '2026-07-01')
                      AND (mwra.end_date IS NULL OR mwra.end_date >= '2026-07-01')
                    ORDER BY mwra.id DESC
                    LIMIT 1
                ), 0.0) AS min_wage_rate
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN msp m ON c.msp_id = m.id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
            LEFT JOIN position pos ON sp.position_id = pos.position_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            WHERE c.status IN :statuses
              AND se.deleted_at IS NULL
              AND se.confirmed = 1
              AND se.cancel_reason = 0
            GROUP BY c.client_id, c.name, c.status, c.msp_id, m.name, e.venue_id, v.name, vp.venue_position_id, pos.description
            ORDER BY c.name, pos.description
        """)

        with engine.begin() as conn:
            rows = conn.execute(sql, {"statuses": RATE_REPORT_STATUSES}).mappings().fetchall()

    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Database error: {str(e)}"})
    finally:
        engine.dispose()

    saved = _load_rate_report_adjustments()
    clients: dict = {}
    for row in rows:
        c_id = row["client_id"]
        c_name = row["client_name"]
        c_status = CLIENT_STATUS_MAP.get(row["client_status"], str(row["client_status"]))

        if c_id not in clients:
            clients[c_id] = {
                "client_id": c_id,
                "client_name": c_name,
                "client_status": c_status,
                "msp_id": row["msp_id"],
                "msp_name": row["msp_name"],
                "positions": []
            }

        avg_pay = round(float(row["avg_pay_rate"] or 0), 2)
        avg_bill = round(float(row["avg_bill_rate"] or 0), 2)
        current_markup = round(((avg_bill / avg_pay) - 1.0) * 100.0, 1) if avg_pay > 0 else 0.0
        vpid = row["venue_position_id"]
        venue_id = row["venue_id"]
        pos_name = row["position_name"] or "Unknown"
        pos_key = f"{c_id}:{vpid}" if vpid else f"{c_id}:v{venue_id}:{pos_name}"
        min_wage = round(float(row["min_wage_rate"] or 0), 2)

        saved_adj = saved.get(pos_key, {})

        # Enforce: saved new_pay_rate must not be below min wage (only apply if position is in saved adjustments)
        if pos_key in saved:
            saved_new_pay = saved_adj.get("new_pay_rate", avg_pay)
            if min_wage > 0:
                saved_new_pay = max(saved_new_pay, min_wage)
            saved_new_bill = saved_adj.get("new_bill_rate", avg_bill)
            saved_new_markup = saved_adj.get("new_markup_pct", current_markup)
        else:
            saved_new_pay = avg_pay
            saved_new_bill = avg_bill
            saved_new_markup = current_markup

        clients[c_id]["positions"].append({
            "key": pos_key,
            "venue_position_id": vpid,
            "client_id": c_id,
            "venue_id": venue_id,
            "venue_name": row["venue_name"] or "",
            "position_name": pos_name,
            "avg_pay_rate": avg_pay,
            "avg_bill_rate": avg_bill,
            "current_markup_pct": current_markup,
            "shift_count": int(row["shift_count"]),
            "last_used": str(row["last_used"]) if row["last_used"] else "",
            "min_wage_rate": min_wage,
            "new_pay_rate": saved_new_pay,
            "new_bill_rate": saved_new_bill,
            "new_markup_pct": saved_new_markup,
        })

    client_list = list(clients.values())
    for c in client_list:
        c["positions"].sort(key=lambda x: x["shift_count"], reverse=True)
    client_list.sort(key=lambda x: x["client_name"])
    return {"clients": client_list, "saved_adjustments": saved}


@router.post("/rate-report/save")
async def save_rate_report_adjustments(payload: RateReportSaveRequest):
    try:
        # Full replace — frontend sends the complete state, so resets/deletions are honoured
        _save_rate_report_adjustments(payload.adjustments)
        return {"status": "ok", "saved": len(payload.adjustments)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Save error: {str(e)}"})


@router.get("/rate-report/download")
async def download_rate_report():
    saved = _load_rate_report_adjustments()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "client_id", "client_name", "venue_position_id", "venue_id",
        "position_name", "current_pay_rate", "current_bill_rate", "current_markup_pct",
        "new_pay_rate", "new_bill_rate", "new_markup_pct", "pay_change", "bill_change"
    ])

    for _key, adj in saved.items():
        orig_pay = adj.get("orig_pay_rate", 0) or 0
        orig_bill = adj.get("orig_bill_rate", 0) or 0
        new_pay = adj.get("new_pay_rate", orig_pay) or orig_pay
        new_bill = adj.get("new_bill_rate", orig_bill) or orig_bill
        writer.writerow([
            adj.get("client_id", ""),
            adj.get("client_name", ""),
            adj.get("venue_position_id", ""),
            adj.get("venue_id", ""),
            adj.get("position_name", ""),
            round(orig_pay, 2),
            round(orig_bill, 2),
            adj.get("orig_markup_pct", ""),
            round(new_pay, 2),
            round(new_bill, 2),
            adj.get("new_markup_pct", ""),
            round(new_pay - orig_pay, 2),
            round(new_bill - orig_bill, 2),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=rate_report.csv"}
    )

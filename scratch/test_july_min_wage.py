import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# Load the environment variables
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

host = os.getenv("DB_HOST")
name = os.getenv("DB_NAME", "cstaffing_live")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT", "3306"))

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

def test_calcs():
    start_date = "2026-04-25"
    end_date = "2026-05-24"
    
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

    # Join min wage as of July 1, 2026
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
        LEFT JOIN min_wage_rate_amount mwra ON v.min_wage_id = mwra.min_wage_id
            AND (mwra.start_date IS NULL OR mwra.start_date <= '2026-07-01')
            AND (mwra.end_date IS NULL OR mwra.end_date >= '2026-07-01')
        WHERE e.date >= :start_date AND e.date <= :end_date
          AND c.msp_id = 2
          AND (
              (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
              OR se.shift_employee_id IN (
                  SELECT shift_employee_id FROM timesheet
                  WHERE client_min_bill = 1 OR employee_min_pay = 1
              )
          )
        """
    )

    params = {"start_date": start_date, "end_date": end_date}

    with engine.begin() as connection:
        df = pd.read_sql(sql, connection, params=params)
        
        try:
            asp_sql = text("SELECT rate, start_date, end_date FROM additional_shift_pay")
            asp_rules_raw = connection.execute(asp_sql).mappings().fetchall()
            asp_rules = [dict(r) for r in asp_rules_raw]
        except Exception:
            asp_rules = []

    print("Fetched", len(df), "rows from DB for Compass")
    if df.empty:
        return
        
    numeric_cols = [
        "client_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
        "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
        "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        "msp_rate", "wc_rate", "min_wage"
    ]
    existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
    if existing_numeric_cols:
        df[existing_numeric_cols] = df[existing_numeric_cols].fillna(0)

    def process_row_sim(row, simulate=False):
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
            # Pay rates reduced by $2, no less than July 1, 2026 min_wage
            pay_rate = max(orig_pay_rate - 2.0, min_wage)
            bill_rate = pay_rate * 1.77
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

    print("\n--- BASELINE CALCULATIONS ---")
    base_calc = df.apply(lambda r: process_row_sim(r, False), axis=1)
    base_bill = float(base_calc["total_bill"].sum())
    base_pay = float(base_calc["total_pay"].sum())
    base_msp = float(base_calc["msp_fee"].sum())
    base_wc = float(base_calc["wc_fee"].sum())
    base_cc = float(base_calc["cc_fee"].sum())
    base_bt = float(base_calc["bt_fee"].sum())
    base_comm = float(base_calc["commissions"].sum())
    base_tax = base_pay * 0.10
    base_profit = base_bill - base_pay - base_msp - base_wc - base_cc - base_bt - base_comm - base_tax

    print(f"Total Bill: ${base_bill:,.2f}")
    print(f"Gross Pay:  ${base_pay:,.2f}")
    print(f"Profit:     ${base_profit:,.2f}")

    print("\n--- SIMULATED CALCULATIONS (July 1 2026 min wage floor) ---")
    sim_calc = df.apply(lambda r: process_row_sim(r, True), axis=1)
    sim_bill = float(sim_calc["total_bill"].sum())
    sim_pay = float(sim_calc["total_pay"].sum())
    sim_msp = float(sim_calc["msp_fee"].sum())
    sim_wc = float(sim_calc["wc_fee"].sum())
    sim_cc = float(sim_calc["cc_fee"].sum())
    sim_bt = float(sim_calc["bt_fee"].sum())
    sim_comm = float(sim_calc["commissions"].sum())
    sim_tax = sim_pay * 0.10
    sim_profit = sim_bill - sim_pay - sim_msp - sim_wc - sim_cc - sim_bt - sim_comm - sim_tax

    print(f"Total Bill: ${sim_bill:,.2f}")
    print(f"Gross Pay:  ${sim_pay:,.2f}")
    print(f"MSP Fee:    ${sim_msp:,.2f}")
    print(f"WC Fee:     ${sim_wc:,.2f}")
    print(f"CC Fee:     ${sim_cc:,.2f}")
    print(f"BT Fee:     ${sim_bt:,.2f}")
    print(f"Comm Fee:   ${sim_comm:,.2f}")
    print(f"Payroll Tax:${sim_tax:,.2f}")
    print(f"Profit:     ${sim_profit:,.2f}")

    profit_diff = sim_profit - base_profit
    print(f"\nProfit Change: ${profit_diff:+,.2f} ({((sim_profit/base_profit)-1)*100:+.1f}%)")

if __name__ == "__main__":
    test_calcs()

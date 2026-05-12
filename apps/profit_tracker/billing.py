from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import inspect, text


TIMESHEET_COLUMNS = [
    "use_sheet",
    "client_seconds",
    "employee_seconds",
    "client_min_bill",
    "client_no_bill",
    "client_no_break_penalty",
    "client_tips",
    "client_parking",
    "client_travel",
    "client_service_charge",
]

NUMERIC_COLUMNS = [
    "client_seconds",
    "employee_seconds",
    "client_tips",
    "client_parking",
    "client_travel",
    "client_service_charge",
    "venue_service_charge",
    "client_no_break_penalty",
    "bill_rate",
]


def _timesheet_select(engine: Any) -> str:
    timesheet_columns = {
        col["name"] for col in inspect(engine).get_columns("timesheet")
    }
    return ", ".join(
        f"t.{col}" if col in timesheet_columns else f"0 AS {col}"
        for col in TIMESHEET_COLUMNS
    )


def load_billable_shift_rows(engine: Any, start_date: str, end_date: str) -> pd.DataFrame:
    ts_select = _timesheet_select(engine)
    sql = text(
        f"""
        SELECT
            e.date,
            c.client_id,
            c.name AS client_name,
            se.bill_rate,
            v.service_charge AS venue_service_charge,
            t.client_worked,
            t.employee_worked,
            s.start AS shift_start,
            s.end AS shift_end,
            t.client_start,
            t.employee_start,
            {ts_select}
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        JOIN client c ON e.client_id = c.client_id
        LEFT JOIN venue v ON e.venue_id = v.venue_id
        LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
        LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        LEFT JOIN shift s ON sp.shift_id = s.shift_id
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
    with engine.begin() as connection:
        return pd.read_sql(
            sql,
            connection,
            params={"start_date": start_date, "end_date": end_date},
        )


def prepare_billing_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    prepared = df.copy()
    existing_numeric_cols = [col for col in NUMERIC_COLUMNS if col in prepared.columns]
    if existing_numeric_cols:
        prepared[existing_numeric_cols] = prepared[existing_numeric_cols].fillna(0)
    return prepared


def calculate_total_bill(row: pd.Series) -> float:
    """Match Profit Tracker's Total Bill calculation for one shift_employee row."""
    use_sheet = str(row.get("use_sheet") or "").upper()
    c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
    e_sec = (
        float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
    )

    if use_sheet == "":
        bill_seconds = c_sec
    elif use_sheet == "EMPLOYEE":
        bill_seconds = e_sec
    else:
        bill_seconds = c_sec

    c_hours = bill_seconds / 3600.0

    shift_start_raw = row.get("shift_start")
    shift_end_raw = row.get("shift_end")
    if pd.notna(shift_start_raw) and pd.notna(shift_end_raw):
        shift_dur_hours = (
            pd.to_datetime(shift_end_raw) - pd.to_datetime(shift_start_raw)
        ).total_seconds() / 3600.0
        shift_min_bill_hours = 4.0 if shift_dur_hours >= 4.0 else 2.0
    else:
        shift_min_bill_hours = 4.0

    shift_start = (
        pd.to_datetime(row.get("shift_start"))
        if pd.notna(row.get("shift_start"))
        else None
    )
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

    c_min = row.get("client_min_bill")
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

    e_worked = str(row.get("employee_worked") or "").upper()
    c_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        c_non_worked = max(c_bill_reg - c_hours, 0.0)
    c_worked_hours = c_bill_reg - c_non_worked

    if c_worked_hours > 12:
        c_dt = c_worked_hours - 12
        c_ot = 4.0
        c_reg = 8.0
    elif c_worked_hours > 8:
        c_dt = 0.0
        c_ot = c_worked_hours - 8
        c_reg = 8.0
    else:
        c_dt = 0.0
        c_ot = 0.0
        c_reg = c_worked_hours

    client_no = row.get("client_no_bill")
    if pd.notna(client_no) and float(client_no) > 0:
        c_reg = c_ot = c_dt = c_non_worked = 0.0

    bill_rate = float(row["bill_rate"])
    reg_bill = c_reg * bill_rate
    ot_bill = c_ot * bill_rate * 1.5
    dt_bill = c_dt * bill_rate * 2.0
    non_worked_bill = c_non_worked * bill_rate

    service_pct_c = float(row.get("client_service_charge") or 0)
    venue_flat = float(row.get("venue_service_charge") or 0)
    service_amt_c = ((reg_bill + ot_bill + dt_bill) * service_pct_c / 100.0) + venue_flat
    meal_amt_c = bill_rate if float(row.get("client_no_break_penalty") or 0) > 0 else 0.0

    c_worked = str(row.get("client_worked") or "").upper()
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

    return (
        reg_bill
        + ot_bill
        + dt_bill
        + non_worked_bill
        + service_amt_c
        + meal_amt_c
        + c_tips
        + c_parking
        + c_travel
    )


def total_bill_by_client(engine: Any, start_date: str, end_date: str) -> dict[int, float]:
    df = load_billable_shift_rows(engine, start_date, end_date)
    if df.empty:
        return {}
    df = prepare_billing_dataframe(df)
    df["total_bill"] = df.apply(calculate_total_bill, axis=1)
    grouped = df.groupby("client_id")["total_bill"].sum()
    return {int(client_id): float(total) for client_id, total in grouped.items()}

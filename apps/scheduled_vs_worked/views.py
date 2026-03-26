from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _db_url_from_env() -> URL:
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

@router.get("", response_class=HTMLResponse)
async def scheduled_vs_worked_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    # weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/scheduled_vs_worked.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

def _normalize_status(s: str | None) -> str:
    if not s:
        return ""
    # Normalize: uppercase, strip, and remove all spaces (handles NOSHOW vs NO SHOW)
    return s.strip().upper().replace(" ", "")

@router.get("/data")
async def get_scheduled_vs_worked_data(
    start_date: str = Query(...),
    end_date: str = Query(...),
    statuses: list[str] = Query([])
):
    engine = _engine()
    try:
        sql = text("""
            SELECT
                e.date,
                e.title AS event_name,
                emp.first_name,
                emp.last_name,
                s.start AS shift_start,
                s.end AS shift_end,
                t.employee_seconds,
                t.employee_worked,
                t.client_worked,
                e.event_id
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
            WHERE e.date >= :start_date
              AND e.date <= :end_date
              AND se.deleted_at IS NULL
              # We only show shifts that are either active confirmed assignments
              # OR explicitly marked in the timesheet with one of our statuses.
              AND (
                  (se.confirmed = 1 AND se.cancel_reason = 0)
                  OR t.employee_worked IS NOT NULL
                  OR t.client_worked IS NOT NULL
              )
            ORDER BY e.date DESC, emp.last_name, emp.first_name
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            
            data = []
            for row in result:
                # --- Status Filtering ---
                emp_status = _normalize_status(row["employee_worked"])
                cli_status = _normalize_status(row["client_worked"])
                
                if statuses is not None:
                    # Normalize selected filters for comparison
                    upper_filters = [_normalize_status(s) for s in statuses]
                    
                    # If the shift has a marked status (employee or client), 
                    # it must match one of the selected filter options to be shown.
                    # If it has no status marks at all, we show it (discrepancy).
                    if emp_status or cli_status:
                        matches_emp = emp_status in upper_filters
                        matches_cli = cli_status in upper_filters
                        if not (matches_emp or matches_cli):
                            continue

                # --- Scheduled Hours Calculation ---
                sched_hours = 0.0
                start_ts = row["shift_start"]
                end_ts = row["shift_end"]
                
                if start_ts and end_ts:
                    delta = end_ts - start_ts
                    total_seconds = delta.total_seconds()
                    sched_hours = total_seconds / 3600.0
                    
                    # Subtract 0.5 for meal break if over 5 hours
                    if total_seconds > 18000: # 5 hours * 3600
                        sched_hours -= 0.5

                # --- Worked Hours Calculation ---
                emp_seconds = float(row["employee_seconds"] or 0)
                worked_hours = emp_seconds / 3600.0
                
                # Filter: only if there is a difference
                # Using a small epsilon for float comparison
                if abs(round(sched_hours, 2) - round(worked_hours, 2)) > 0.01:
                    # Display the status that was found (prefers employee status if both exist)
                    display_status = row["employee_worked"] or row["client_worked"] or ""
                    
                    data.append({
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "event_name": row["event_name"],
                        "employee_name": f"{row['first_name']} {row['last_name']}",
                        "worked_hours": round(worked_hours, 2),
                        "scheduled_hours": round(sched_hours, 2),
                        "difference": round(worked_hours - sched_hours, 2),
                        "status": display_status,
                        "event_link": f"https://golive.culinarystaffing.com/events/{row['event_id']}/timesheets"
                    })
                
            return JSONResponse({"data": data})
            
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

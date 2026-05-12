from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

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
async def meal_penalty_report_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/meal_penalty_report.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

@router.get("/data")
async def get_meal_penalty_report_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                emp.employee_id,
                emp.first_name,
                emp.last_name,
                t.timesheet_id,
                IFNULL(t.employee_no_break_penalty, 0) AS has_penalty,
                CASE 
                    WHEN t.employee_break_start IS NOT NULL 
                     AND t.employee_start IS NOT NULL
                     AND t.employee_end IS NOT NULL
                     AND TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_break_start) > 300
                     AND IFNULL(e.state, '') NOT IN ('NV', 'WA')
                     AND (
                         TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_end) - 
                         IFNULL(TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end), 0)
                     ) >= 360
                    THEN 1 
                    ELSE 0 
                END AS is_late_break
            FROM timesheet t
            JOIN employee emp ON t.employee_id = emp.employee_id
            JOIN event e ON t.event_id = e.event_id
            WHERE e.date >= :start_date 
              AND e.date <= :end_date
              AND (
                  t.employee_no_break_penalty = 1 
               OR (
                      t.employee_break_start IS NOT NULL 
                  AND t.employee_start IS NOT NULL
                  AND t.employee_end IS NOT NULL
                  AND TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_break_start) > 300
                  AND IFNULL(e.state, '') NOT IN ('NV', 'WA')
                  AND (
                      TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_end) - 
                      IFNULL(TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end), 0)
                  ) >= 360
               )
              )
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            
            employees = {}
            for row in result:
                emp_id = row["employee_id"]
                if emp_id not in employees:
                    employees[emp_id] = {
                        "employee_id": emp_id,
                        "employee_name": f"{row['first_name']} {row['last_name']}",
                        "penalty_count": 0,
                        "late_break_count": 0,
                        "both_count": 0,
                    }
                
                p = int(row["has_penalty"])
                l = int(row["is_late_break"])
                
                if p:
                    employees[emp_id]["penalty_count"] += 1
                if l:
                    employees[emp_id]["late_break_count"] += 1
                if p and l:
                    employees[emp_id]["both_count"] += 1
            
            # Sort employees alphabetically by name
            data = sorted(employees.values(), key=lambda x: x["employee_name"])
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/history/{employee_id}")
async def get_employee_history(employee_id: int):
    engine = _engine()
    try:
        # Construct the last 12 months ending in the current month
        today = datetime.now()
        months = []
        for i in range(11, -1, -1):
            m = today.month - 1 - i
            y = today.year
            while m < 0:
                m += 12
                y -= 1
            dt = datetime(y, m + 1, 1)
            months.append({
                "year": y,
                "month": m + 1,
                "label": dt.strftime("%b %Y"),
                "start_date": dt.strftime("%Y-%m-01"),
            })
        
        start_date = months[0]["start_date"]
        end_date = today.strftime("%Y-%m-%d")
        
        sql_emp = text("SELECT first_name, last_name FROM employee WHERE employee_id = :emp_id")
        
        sql_history = text("""
            SELECT 
                e.date,
                IFNULL(t.employee_no_break_penalty, 0) AS has_penalty,
                CASE 
                    WHEN t.employee_break_start IS NOT NULL 
                     AND t.employee_start IS NOT NULL
                     AND t.employee_end IS NOT NULL
                     AND TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_break_start) > 300
                     AND IFNULL(e.state, '') NOT IN ('NV', 'WA')
                     AND (
                         TIMESTAMPDIFF(MINUTE, t.employee_start, t.employee_end) - 
                         IFNULL(TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end), 0)
                     ) >= 360
                    THEN 1 
                    ELSE 0 
                END AS is_late_break
            FROM timesheet t
            JOIN event e ON t.event_id = e.event_id
            WHERE t.employee_id = :emp_id
              AND e.date >= :start_date 
              AND e.date <= :end_date
        """)
        
        with engine.begin() as conn:
            emp_row = conn.execute(sql_emp, {"emp_id": employee_id}).mappings().first()
            employee_name = f"{emp_row['first_name']} {emp_row['last_name']}" if emp_row else f"Employee #{employee_id}"
            
            result = conn.execute(sql_history, {
                "emp_id": employee_id,
                "start_date": start_date,
                "end_date": end_date
            }).mappings().all()
            
            history_map = {
                (m["year"], m["month"]): {"label": m["label"], "penalties": 0, "late_breaks": 0}
                for m in months
            }
            
            for row in result:
                d = row["date"]
                if hasattr(d, "year") and hasattr(d, "month"):
                    key = (d.year, d.month)
                    if key in history_map:
                        if int(row["has_penalty"]):
                            history_map[key]["penalties"] += 1
                        if int(row["is_late_break"]):
                            history_map[key]["late_breaks"] += 1
            
            labels = [m["label"] for m in months]
            penalties_data = [history_map[(m["year"], m["month"])]["penalties"] for m in months]
            late_breaks_data = [history_map[(m["year"], m["month"])]["late_breaks"] for m in months]
            
            return JSONResponse({
                "employee_name": employee_name,
                "labels": labels,
                "penalties": penalties_data,
                "late_breaks": late_breaks_data
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

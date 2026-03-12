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
async def meal_penalty_dashboard_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    # weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/meal_penalty_dashboard.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

@router.get("/data")
async def get_meal_penalty_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                e.date,
                c.name AS client_name,
                v.name AS venue_name,
                emp.first_name,
                emp.last_name,
                t.employee_start,
                t.employee_end,
                t.employee_break_start,
                t.employee_break_end,
                t.employee_sec_break_start,
                t.employee_sec_break_end,
                t.employee_no_sec_break_reason,
                TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end) AS break_minutes,
                TIMESTAMPDIFF(MINUTE, t.employee_sec_break_start, t.employee_sec_break_end) AS sec_break_minutes,
                t.employee_no_break_penalty,
                t.client_no_break_penalty
            FROM timesheet t
            JOIN employee emp ON t.employee_id = emp.employee_id
            JOIN event e ON t.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            JOIN venue v ON e.venue_id = v.venue_id
            WHERE e.date >= :start_date 
              AND e.date <= :end_date
              AND t.employee_break_start IS NOT NULL 
              AND t.employee_break_end IS NOT NULL
              AND (t.employee_no_break_penalty = 1 OR t.client_no_break_penalty = 1)
              AND TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end) >= 30
            ORDER BY e.date DESC, c.name ASC
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            
            data = []
            for row in result:
                item = dict(row)
                
                # Calculate Worked Hours
                # Formula: (End - Start) - (1st Break) - (2nd Break)
                total_duration_minutes = 0
                if item["employee_start"] and item["employee_end"]:
                    duration = item["employee_end"] - item["employee_start"]
                    total_duration_minutes = duration.total_seconds() / 60
                
                break1_mins = item["break_minutes"] or 0
                break2_mins = item["sec_break_minutes"] or 0
                
                worked_minutes = total_duration_minutes - break1_mins - break2_mins
                worked_hours = round(worked_minutes / 60, 2)
                
                # Exclusion Logic:
                # remove from consideration shifts where an employee worked greater than 10 hours 
                # and employee_sec_break_end and employee_sec_break_start are blank or less than 30 minutes 
                # and employee_no_sec_break_reason is 3. 
                
                is_over_10_hours = worked_hours > 10
                is_sec_break_short_or_missing = (break2_mins < 30)
                is_reason_3 = (item["employee_no_sec_break_reason"] == 3)
                
                if is_over_10_hours and is_sec_break_short_or_missing and is_reason_3:
                    continue
                
                item["employee_name"] = f"{item['first_name']} {item['last_name']}"
                item["date"] = item["date"].strftime("%Y-%m-%d")
                item["employee_penalty"] = "Yes" if item["employee_no_break_penalty"] == 1 else "No"
                item["client_penalty"] = "Yes" if item["client_no_break_penalty"] == 1 else "No"
                item["worked_hours"] = worked_hours
                
                # Clean up items not needed for frontend
                for key in list(item.keys()):
                    if key not in ["date", "client_name", "venue_name", "employee_name", 
                                  "break_minutes", "employee_penalty", "client_penalty", "worked_hours"]:
                        item.pop(key)
                
                data.append(item)
                
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

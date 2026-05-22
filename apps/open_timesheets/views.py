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
async def open_timesheets_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/open_timesheets.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

@router.get("/data")
async def get_open_timesheets_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                ev.date AS event_date,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                ev.event_id,
                t.employee_start,
                t.employee_end,
                t.employee_break_start,
                t.employee_break_end,
                COALESCE(CONCAT(u.first_name, ' ', u.last_name), 'No Staffing Coordinator') AS staffing_coordinator
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN employee e ON t.employee_id = e.employee_id
            JOIN event ev ON t.event_id = ev.event_id
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            LEFT JOIN user u ON v.sales_rep_id = u.id
            WHERE se.cancel_reason = 0 
              AND se.deleted_at IS NULL
              AND ev.deleted_at IS NULL
              AND ev.timeclock IN ('EMPLOYEE_CODE', 'DISABLED')
              AND (
                  (t.employee_start IS NULL OR t.employee_end IS NULL)
                  OR (
                      TIMESTAMPDIFF(SECOND, t.employee_start, t.employee_end) > 18000
                      AND (t.employee_break_start IS NULL OR t.employee_break_end IS NULL)
                      AND (t.employee_no_break_reason IS NULL OR t.employee_no_break_reason NOT REGEXP '^[0-9]+$')
                  )
              )
              AND (t.client_worked NOT IN ('NOSHOW', 'SENTHOME') OR t.client_worked IS NULL)
              AND ev.date >= :start_date 
              AND ev.date <= :end_date
            ORDER BY ev.date ASC, employee_name ASC
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            
            data = []
            for row in result:
                item = dict(row)
                if item["event_date"]:
                    try:
                        item["event_date"] = item["event_date"].strftime("%Y-%m-%d")
                    except AttributeError:
                        item["event_date"] = str(item["event_date"])
                else:
                    item["event_date"] = ""

                # Format times
                for time_field in ['employee_start', 'employee_end', 'employee_break_start', 'employee_break_end']:
                    if item.get(time_field) and isinstance(item[time_field], datetime):
                        item[time_field] = item[time_field].strftime("%Y-%m-%d %H:%M:%S")
                    elif not item.get(time_field):
                        item[time_field] = ""
                        
                item["employee_name"] = item["employee_name"] if item["employee_name"] else "Unknown Employee"
                item["timesheet_link"] = f"https://golive.culinarystaffing.com/events/{item['event_id']}/timesheets"
                
                data.append(item)
                
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

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
    # Default date range: Today only
    tz = ZoneInfo(os.getenv("APP_TIMEZONE", "America/Los_Angeles"))
    today = datetime.now(tz)
    
    start_date = today.strftime("%Y-%m-%d")
    end_date = start_date
    
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
                c.name AS client_name,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                e.mobile AS employee_phone,
                ev.event_id,
                ev.timeclock AS timeclock,
                s.start AS shift_start,
                t.employee_start,
                t.employee_end,
                t.employee_break_start,
                t.employee_break_end,
                COALESCE(CONCAT(u.first_name, ' ', u.last_name), 'No Staffing Coordinator') AS staffing_coordinator
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            JOIN employee e ON t.employee_id = e.employee_id
            JOIN event ev ON t.event_id = ev.event_id
            JOIN client c ON ev.client_id = c.client_id
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            LEFT JOIN user u ON v.sales_rep_id = u.id
            WHERE se.cancel_reason = 0 
              AND se.deleted_at IS NULL
              AND ev.deleted_at IS NULL
              AND ev.timeclock IN ('EMPLOYEE_CODE', 'DISABLED', 'ATLAS', 'NOWSTA', 'EMPLOYEE')
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
            tz = ZoneInfo(os.getenv("APP_TIMEZONE", "America/Los_Angeles"))
            today_date_str = datetime.now(tz).strftime("%Y-%m-%d")
            # Convert now to naive datetime to match the naive shift_start_dt and employee_start from DB
            now_dt = datetime.now(tz).replace(tzinfo=None)
            
            for row in result:
                item = dict(row)
                event_date_str = ""
                if item["event_date"]:
                    try:
                        event_date_str = item["event_date"].strftime("%Y-%m-%d")
                        item["event_date"] = event_date_str
                    except AttributeError:
                        event_date_str = str(item["event_date"])
                        item["event_date"] = event_date_str
                else:
                    item["event_date"] = ""

                # Format shift_start
                shift_start_time = item.get("shift_start")
                shift_start_dt = None
                if shift_start_time is not None and event_date_str:
                    if isinstance(shift_start_time, timedelta):
                        total_seconds = int(shift_start_time.total_seconds())
                        hours = total_seconds // 3600
                        minutes = (total_seconds % 3600) // 60
                        item["shift_start"] = f"{hours:02d}:{minutes:02d}:00"
                        shift_start_dt = datetime.strptime(f"{event_date_str} {item['shift_start']}", "%Y-%m-%d %H:%M:%S")
                    elif hasattr(shift_start_time, "strftime"):
                        item["shift_start"] = shift_start_time.strftime("%H:%M:%S")
                        shift_start_dt = datetime.strptime(f"{event_date_str} {item['shift_start']}", "%Y-%m-%d %H:%M:%S")
                    else:
                        item["shift_start"] = str(shift_start_time)
                else:
                    item["shift_start"] = ""

                # Late calculation for today
                item["minutes_late"] = ""
                if event_date_str == today_date_str and shift_start_dt:
                    employee_start_dt = item.get("employee_start")
                    if employee_start_dt and isinstance(employee_start_dt, datetime):
                        diff = employee_start_dt - shift_start_dt
                        item["minutes_late"] = int(diff.total_seconds() / 60)
                    elif not employee_start_dt:
                        diff = now_dt - shift_start_dt
                        item["minutes_late"] = int(diff.total_seconds() / 60)

                # Format times
                for time_field in ['employee_start', 'employee_end', 'employee_break_start', 'employee_break_end']:
                    if item.get(time_field) and isinstance(item[time_field], datetime):
                        item[time_field] = item[time_field].strftime("%Y-%m-%d %H:%M:%S")
                    elif not item.get(time_field):
                        item[time_field] = ""
                        
                item["employee_name"] = item["employee_name"] if item["employee_name"] else "Unknown Employee"
                item["employee_phone"] = item.get("employee_phone") or ""
                item["client_name"] = item.get("client_name") or "Unknown Client"
                item["timesheet_link"] = f"https://golive.culinarystaffing.com/events/{item['event_id']}/timesheets"
                
                data.append(item)
                
            def sort_key(item):
                ml = item.get("minutes_late", "")
                if ml == "":
                    return (2, item.get("event_date", ""), item.get("employee_name", ""))
                if ml > 0:
                    return (0, ml, item.get("employee_name", ""))
                else:
                    return (1, -ml, item.get("employee_name", ""))
            
            data.sort(key=sort_key)
                
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

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
async def payroll_notes_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/payroll_notes.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

@router.get("/data")
async def get_payroll_notes_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                en.datetime AS date,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                en.note,
                CONCAT(u.first_name, ' ', u.last_name) AS user_name,
                ev.title AS event_name,
                ev.date AS event_date,
                ev.event_id,
                en.employee_id,
                'Payroll Note' AS type
            FROM employee_note en
            LEFT JOIN employee e ON en.employee_id = e.employee_id
            LEFT JOIN user u ON en.user_id = u.id
            LEFT JOIN shift_employee se ON en.shift_employee_id = se.shift_employee_id
            LEFT JOIN event ev ON se.event_id = ev.event_id
            WHERE UPPER(TRIM(en.type)) LIKE '%PAYROLL%'
              AND DATE(en.datetime) >= :start_date 
              AND DATE(en.datetime) <= :end_date

            UNION ALL

            SELECT 
                COALESCE(t.employee_submit_date, ev.date) AS date,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                t.employee_notes AS note,
                'Employee' AS user_name,
                ev.title AS event_name,
                ev.date AS event_date,
                ev.event_id,
                t.employee_id,
                'Timesheet Note' AS type
            FROM timesheet t
            JOIN employee e ON t.employee_id = e.employee_id
            JOIN event ev ON t.event_id = ev.event_id
            WHERE t.employee_notes IS NOT NULL AND t.employee_notes != ''
              AND DATE(COALESCE(t.employee_submit_date, ev.date)) >= :start_date 
              AND DATE(COALESCE(t.employee_submit_date, ev.date)) <= :end_date

            UNION ALL

            SELECT 
                COALESCE(t.client_submit_date, ev.date) AS date,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                t.client_notes AS note,
                'Client' AS user_name,
                ev.title AS event_name,
                ev.date AS event_date,
                ev.event_id,
                t.employee_id,
                'Timesheet Note' AS type
            FROM timesheet t
            JOIN employee e ON t.employee_id = e.employee_id
            JOIN event ev ON t.event_id = ev.event_id
            WHERE t.client_notes IS NOT NULL AND t.client_notes != ''
              AND DATE(COALESCE(t.client_submit_date, ev.date)) >= :start_date 
              AND DATE(COALESCE(t.client_submit_date, ev.date)) <= :end_date

            ORDER BY date DESC
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            
            data = []
            for row in result:
                item = dict(row)
                if isinstance(item["date"], datetime):
                    item["date"] = item["date"].strftime("%Y-%m-%d %H:%M")
                elif item["date"]:
                    # Fallback for date objects (like ev.date)
                    try:
                        item["date"] = item["date"].strftime("%Y-%m-%d")
                    except AttributeError:
                        item["date"] = str(item["date"])
                else:
                    item["date"] = ""
                item["employee_name"] = item["employee_name"] if item["employee_name"] else "Unknown Employee"
                item["user_name"] = item["user_name"] if item["user_name"] else "Unknown User"
                
                if item["event_date"]:
                    item["event_date"] = item["event_date"].strftime("%Y-%m-%d")
                    item["shift_display"] = f"{item['event_name']} ({item['event_date']})"
                else:
                    item["event_date"] = ""
                    item["shift_display"] = "No Shift Linked"
                
                data.append(item)
                
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

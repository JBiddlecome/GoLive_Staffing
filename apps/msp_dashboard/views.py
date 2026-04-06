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
async def msp_dashboard_page(request: Request):
    return templates.TemplateResponse(
        "apps/msp_dashboard.html",
        {"request": request}
    )

@router.get("/data")
async def get_msp_dashboard_data():
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                se.employee_id,
                ev.event_id,
                e.first_name, 
                e.last_name, 
                IFNULL(NULLIF(ev.title, ''), v.name) AS event_name, 
                c.msp_id,
                m.name AS msp_name, 
                s.start AS shift_date,
                se.confirmed_at
            FROM shift_employee se
            JOIN employee e ON se.employee_id = e.employee_id AND e.deleted_at IS NULL
            JOIN event ev ON se.event_id = ev.event_id AND ev.deleted_at IS NULL
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            JOIN client c ON ev.client_id = c.client_id AND c.deleted_at IS NULL
            JOIN msp m ON c.msp_id = m.id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            WHERE 
                se.confirmed = 1 
                AND c.msp_id IN (3, 5, 20)
                AND se.deleted_at IS NULL
                AND se.confirmed_at >= :start_of_day
            ORDER BY se.confirmed_at DESC;
        """)
        
        start_of_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"start_of_day": start_of_day}).mappings().all()
            
            data = []
            for row in result:
                item = dict(row)
                item["employee_name"] = f"{item['first_name']} {item['last_name']}"
                if item["shift_date"]:
                    item["shift_date"] = item["shift_date"].strftime("%Y-%m-%d %H:%M")
                if item["confirmed_at"]:
                    item["confirmed_at"] = item["confirmed_at"].strftime("%Y-%m-%d %H:%M:%S")
                
                data.append({
                    "employee_id": item["employee_id"],
                    "event_id": item["event_id"],
                    "employee_name": item["employee_name"],
                    "event_name": item["event_name"],
                    "msp_id": item["msp_id"],
                    "msp_name": item["msp_name"],
                    "shift_date": item["shift_date"],
                    "confirmed_at": item["confirmed_at"]
                })
                
            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/log")
async def get_msp_dashboard_log():
    engine = _engine()
    try:
        with engine.begin() as connection:
            # 1. Get DB Time
            db_now = connection.execute(text("SELECT NOW()")).scalar()
            
            # 2. Get latest 50 confirmations across all MSPs
            sql_recent = text("""
                SELECT 
                    e.first_name, 
                    e.last_name, 
                    m.name AS msp_name,
                    se.confirmed_at
                FROM shift_employee se
                JOIN employee e ON se.employee_id = e.employee_id AND e.deleted_at IS NULL
                JOIN event ev ON se.event_id = ev.event_id AND ev.deleted_at IS NULL
                JOIN client c ON ev.client_id = c.client_id AND c.deleted_at IS NULL
                JOIN msp m ON c.msp_id = m.id
                WHERE 
                    se.confirmed = 1 
                    AND c.msp_id IN (3, 5, 20)
                    AND se.deleted_at IS NULL
                ORDER BY se.confirmed_at DESC
                LIMIT 50;
            """)
            result_recent = connection.execute(sql_recent).mappings().all()
            latest = [dict(r) for r in result_recent]
            for r in latest:
                if r["confirmed_at"]: r["confirmed_at"] = r["confirmed_at"].strftime("%Y-%m-%d %H:%M:%S")
            
            log_data = {
                "database_time": db_now.strftime("%Y-%m-%d %H:%M:%S") if db_now else None,
                "latest_msp_confirmations": latest
            }
            return JSONResponse(log_data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

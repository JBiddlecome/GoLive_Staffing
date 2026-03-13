from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    missing = [
        env_name
        for env_name, value in (
            ("DB_HOST", host),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required database environment variables: {', '.join(missing)}",
        )

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
async def unconfirmed_requests_page(request: Request):
    # Default date range: today to 2 weeks from now
    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    two_weeks_later = today + timedelta(days=14)
    
    return templates.TemplateResponse(
        "apps/unconfirmed_requests.html", 
        {
            "request": request,
            "default_start": today.isoformat(),
            "default_end": two_weeks_later.isoformat()
        }
    )

@router.get("/data", response_class=JSONResponse)
async def unconfirmed_requests_data(
    start_date: str = Query(None),
    end_date: str = Query(None),
    staffing_manager_id: str = Query(None)
):
    engine = _engine()
    try:
        # Fetch data for unconfirmed requests
        sql = text("""
            SELECT 
                se.created_at,
                emp.first_name,
                emp.last_name,
                e.date as event_date,
                c.name as client_name,
                v.name as venue_name,
                CONCAT(u_mgr.first_name, ' ', u_mgr.last_name) as staffing_manager_name,
                v.staffing_manager_id,
                CONCAT(u_req.first_name, ' ', u_req.last_name) as requester_name,
                u_req.id as requester_id
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            JOIN venue v ON e.venue_id = v.venue_id
            JOIN employee emp ON se.employee_id = emp.employee_id
            LEFT JOIN user u_req ON se.request_by = u_req.id
            LEFT JOIN user u_mgr ON v.staffing_manager_id = u_mgr.id
            WHERE se.created_at IS NOT NULL
              AND se.confirmed_at IS NULL
              AND se.cancelled_at IS NULL
              AND u_req.`group` = 'EMPLOYEE'
              AND e.date >= :start_date
              AND e.date <= :end_date
        """)

        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
            
            # Fetch staffing managers who have unconfirmed requests within the date range
            mgr_sql = text("""
                SELECT DISTINCT
                    v.staffing_manager_id,
                    CONCAT(u.first_name, ' ', u.last_name) as staffing_manager_name
                FROM shift_employee se
                JOIN event e ON se.event_id = e.event_id
                JOIN venue v ON e.venue_id = v.venue_id
                JOIN user u ON v.staffing_manager_id = u.id
                LEFT JOIN user u_req ON se.request_by = u_req.id
                WHERE se.created_at IS NOT NULL
                  AND se.confirmed_at IS NULL
                  AND se.cancelled_at IS NULL
                  AND u_req.`group` = 'EMPLOYEE'
                  AND e.date >= :start_date
                  AND e.date <= :end_date
                ORDER BY staffing_manager_name
            """)
            mgr_df = pd.read_sql(mgr_sql, conn, params=params)
            staffing_managers = mgr_df.to_dict(orient='records')

        if staffing_manager_id:
            try:
                mgr_id_int = int(staffing_manager_id)
                df = df[df['staffing_manager_id'] == mgr_id_int]
            except ValueError:
                pass

        # Calculate hours passed
        now = datetime.now(ZoneInfo("America/Los_Angeles")).replace(tzinfo=None)
        df['hours_passed'] = df['created_at'].apply(
            lambda x: round((now - x).total_seconds() / 3600, 1) if pd.notnull(x) else 0
        )
        
        # Sort by hours_passed descending
        df = df.sort_values(by='hours_passed', ascending=False)
        
        # Clean up for JSON
        df['created_at'] = df['created_at'].astype(str)
        df['event_date'] = df['event_date'].astype(str)
        
        records = df.to_dict(orient='records')

        return JSONResponse({
            "status": "success",
            "data": records,
            "staffing_managers": staffing_managers
        })

    except Exception as e:
        # Include the full error message for easier debugging
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

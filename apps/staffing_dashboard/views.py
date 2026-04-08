from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from .notes_data import load_notes, update_note
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
async def staffing_dashboard_page(request: Request):
    return templates.TemplateResponse("apps/staffing_dashboard.html", {"request": request})

@router.get("/data", response_class=JSONResponse)
async def staffing_dashboard_data():
    engine = _engine()
    try:
        # 1. Fetch current and upcoming metrics
        metrics_sql = text("""
            SELECT
                c.client_id,
                c.name AS client_name,
                CONCAT(u.first_name, ' ', u.last_name) AS staffing_manager_name,
                COUNT(DISTINCT e.event_id) AS event_count,
                SUM(sp.count) AS total_shifts,
                SUM(IFNULL(se_counts.filled_cnt, 0)) AS filled_shifts,
                SUM(IFNULL(se_counts.requested_cnt, 0)) AS requested_shifts
            FROM client c
            JOIN event e ON c.client_id = e.client_id
            JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN user u ON v.staffing_manager_id = u.id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            LEFT JOIN (
                SELECT 
                    shift_position_id,
                    COUNT(CASE WHEN confirmed = 1 AND cancel_reason = 0 THEN shift_employee_id END) AS filled_cnt,
                    COUNT(CASE WHEN confirmed_at IS NULL AND cancelled_at IS NULL AND created_at IS NOT NULL THEN shift_employee_id END) AS requested_cnt
                FROM shift_employee
                GROUP BY shift_position_id
            ) se_counts ON sp.shift_position_id = se_counts.shift_position_id
            WHERE e.date >= CURDATE()
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY c.client_id, c.name, staffing_manager_name
            ORDER BY client_name
        """)

        # 2. Fetch average time to fill (last 6 months)
        fill_time_sql = text("""
            SELECT
                c.client_id,
                AVG(TIMESTAMPDIFF(SECOND, se.created_at, se.confirmed_at)) AS avg_fill_seconds
            FROM client c
            JOIN event e ON c.client_id = e.client_id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id
            WHERE se.confirmed = 1
              AND se.confirmed_at IS NOT NULL
              AND se.created_at >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY c.client_id
        """)

        with engine.connect() as conn:
            metrics_df = pd.read_sql(metrics_sql, conn)
            fill_time_df = pd.read_sql(fill_time_sql, conn)

        # Merge data
        if metrics_df.empty:
            return JSONResponse({
                "status": "success",
                "data": []
            })

        df = metrics_df.merge(fill_time_df, on='client_id', how='left')

        # Calculations
        df['fill_rate'] = (df['filled_shifts'] / df['total_shifts'] * 100).fillna(0).round(1)
        
        def format_duration(seconds):
            if pd.isna(seconds) or seconds < 0:
                return "No Data"
            
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            
            if hours > 0:
                return f"{hours}h {minutes}m"
            else:
                return f"{minutes}m"

        df['avg_time_to_fill'] = df['avg_fill_seconds'].apply(format_duration)
        
        df = df.fillna(0)
        records = df.to_dict(orient='records')

        notes = load_notes()
        for record in records:
            record['client_note'] = notes.get(str(record['client_id']), "")

        return JSONResponse({
            "status": "success",
            "data": records
        })

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/notes/{client_id}", response_class=JSONResponse)
async def update_staffing_dashboard_note(client_id: int, payload: dict = Body(...)):
    note = payload.get('note', "")
    try:
        update_note(str(client_id), note)
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

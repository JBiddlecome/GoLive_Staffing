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
    user_data = request.session.get("user")
    logged_in_email = user_data.get("email") if user_data else None
    logged_in_manager = ""
    staffing_managers = []
    
    engine = _engine()
    try:
        with engine.connect() as conn:
            if logged_in_email:
                check_sql = text("""
                    SELECT CONCAT_WS(' ', u.first_name, u.last_name) as full_name
                    FROM venue v 
                    JOIN user u ON v.staffing_manager_id = u.id 
                    WHERE u.email = :email 
                    LIMIT 1
                """)
                res = conn.execute(check_sql, {"email": logged_in_email}).fetchone()
                if res:
                    logged_in_manager = res[0]
                    
            # Get list of unique staffing managers
            managers_sql = text("""
                SELECT DISTINCT CONCAT(u.first_name, ' ', u.last_name) AS full_name
                FROM event e
                JOIN venue v ON e.venue_id = v.venue_id
                JOIN user u ON v.staffing_manager_id = u.id
                WHERE e.date >= CURDATE()
                  AND e.deleted_at IS NULL
                  AND u.first_name IS NOT NULL AND u.last_name IS NOT NULL
                ORDER BY full_name
            """)
            staffing_managers = [row[0] for row in conn.execute(managers_sql).fetchall()]
    except Exception:
        pass
    finally:
        engine.dispose()

    return templates.TemplateResponse(
        "apps/staffing_dashboard.html", 
        {
            "request": request, 
            "logged_in_manager": logged_in_manager,
            "staffing_managers": staffing_managers
        }
    )

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
                SUM(IFNULL(se_counts.requested_cnt, 0)) AS requested_shifts,
                MIN(CASE WHEN IFNULL(se_counts.filled_cnt, 0) < sp.count THEN e.date END) AS next_unfilled_date,
                SUM(CASE WHEN IFNULL(se_counts.filled_cnt, 0) < sp.count THEN (sp.count - IFNULL(se_counts.filled_cnt, 0)) ELSE 0 END) AS unfilled_shifts
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
        
        if 'next_unfilled_date' in df.columns:
            df['next_unfilled_date'] = df['next_unfilled_date'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        
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

@router.get("/client/{client_id}/details", response_class=JSONResponse)
async def staffing_dashboard_client_details(client_id: int):
    engine = _engine()
    try:
        client_sql = text("""
            SELECT
                e.event_id,
                e.date,
                e.title,
                s.shift_id,
                s.start,
                s.end,
                sp.shift_position_id,
                sp.count AS position_count,
                IFNULL(se_counts.filled_cnt, 0) AS filled_count,
                GROUP_CONCAT(DISTINCT p.`to` SEPARATOR ', ') AS publish_types
            FROM event e
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            LEFT JOIN (
                SELECT 
                    shift_position_id,
                    COUNT(CASE WHEN confirmed = 1 AND cancel_reason = 0 THEN shift_employee_id END) AS filled_cnt
                FROM shift_employee
                GROUP BY shift_position_id
            ) se_counts ON sp.shift_position_id = se_counts.shift_position_id
            LEFT JOIN publishing_shift ps ON s.shift_id = ps.shift_id
            LEFT JOIN publishing p ON ps.publishing_id = p.id
            WHERE e.client_id = :client_id
              AND e.date >= CURDATE()
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY e.event_id, s.shift_id, sp.shift_position_id
            ORDER BY e.date ASC, s.start ASC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(client_sql, conn, params={"client_id": client_id})
            
        if df.empty:
            return JSONResponse({
                "status": "success",
                "data": []
            })
            
        # Group by event
        events_dict = {}
        for _, row in df.iterrows():
            # Skip shifts with position_count == 0
            if row['position_count'] == 0:
                continue
                
            event_id = row['event_id']
            if event_id not in events_dict:
                events_dict[event_id] = {
                    "event_id": event_id,
                    "date": row['date'].isoformat() if pd.notnull(row['date']) else None,
                    "title": row['title'],
                    "shifts": []
                }
            
            shift_data = {
                "shift_id": row['shift_id'],
                "start": row['start'].isoformat() if pd.notnull(row['start']) else None,
                "end": row['end'].isoformat() if pd.notnull(row['end']) else None,
                "position_count": int(row['position_count']),
                "filled_count": int(row['filled_count']),
                "publish_types": row['publish_types'] if pd.notnull(row['publish_types']) else None
            }
            events_dict[event_id]["shifts"].append(shift_data)
            
        # Filter out events with no shifts
        result_data = [e for e in events_dict.values() if len(e["shifts"]) > 0]
        
        return JSONResponse({
            "status": "success",
            "data": result_data
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

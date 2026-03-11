from __future__ import annotations

import os
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
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


@router.get("/", response_class=HTMLResponse)
async def open_shifts_map_index(request: Request):
    return templates.TemplateResponse("apps/open_shifts_map.html", {"request": request})


@router.get("/positions")
async def get_available_positions(start_date: str, end_date: str):
    engine = _engine()
    try:
        sql = text(
            """
            SELECT DISTINCT p.position_id, p.description
            FROM event e
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            JOIN position p ON sp.position_id = p.position_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL AND s.deleted_at IS NULL
            ORDER BY p.description
            """
        )
        with engine.connect() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date})
            positions = [{"id": row[0], "name": row[1]} for row in result]
            return JSONResponse(content={"positions": positions})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()


@router.get("/staffing-managers")
async def get_staffing_managers(start_date: str, end_date: str):
    engine = _engine()
    try:
        sql = text(
            """
            SELECT DISTINCT u.id, CONCAT(u.first_name, ' ', u.last_name) as name
            FROM venue v
            JOIN user u ON v.staffing_manager_id = u.id
            JOIN event e ON v.venue_id = e.venue_id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.cancel_reason = 0
            WHERE v.deleted_at IS NULL AND u.status = 10
              AND e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL AND s.deleted_at IS NULL
              AND se.shift_employee_id IS NULL
            ORDER BY name
            """
        )
        with engine.connect() as connection:
            result = connection.execute(sql, {"start_date": start_date, "end_date": end_date})
            managers = [{"id": row[0], "name": row[1]} for row in result]
            return JSONResponse(content={"managers": managers})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()


@router.get("/shifts")
async def get_open_shifts(
    start_date: str, 
    end_date: str, 
    position_id: int | None = None,
    staffing_manager_id: int | None = None
):
    engine = _engine()
    try:
        sql_text = """
            SELECT
                e.latitude, e.longitude, c.name AS client_name, v.name AS venue_name,
                p.description AS position, sp.rate, e.title as event_title,
                DATE_FORMAT(e.date, '%Y-%m-%d') as event_date,
                CONCAT(u.first_name, ' ', u.last_name) as staffing_manager_name
            FROM event e
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            JOIN position p ON sp.position_id = p.position_id
            JOIN client c ON e.client_id = c.client_id
            JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN user u ON v.staffing_manager_id = u.id
            LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.cancel_reason = 0
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL AND s.deleted_at IS NULL
              AND se.shift_employee_id IS NULL
        """
        params = {"start_date": start_date, "end_date": end_date}
        
        if position_id:
            sql_text += " AND p.position_id = :position_id"
            params["position_id"] = position_id
            
        if staffing_manager_id:
            sql_text += " AND v.staffing_manager_id = :staffing_manager_id"
            params["staffing_manager_id"] = staffing_manager_id
            
        sql = text(sql_text)
        
        with engine.connect() as connection:
            df = pd.read_sql(sql, connection, params=params)
            
            # Debug: Print how many shifts found before filtering
            print(f"DEBUG: Found {len(df)} open shifts for range {start_date} to {end_date}")

            if df.empty:
                return JSONResponse(content={"shifts": []})

            # Clean up coordinates
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            
            df = df.dropna(subset=['latitude', 'longitude'])
            df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]
            
            # Debug: Print how many shifts after coord filtering
            print(f"DEBUG: Found {len(df)} open shifts with valid coordinates")
            
            shifts = df.to_dict(orient="records")
            return JSONResponse(content={"shifts": shifts})
    except Exception as e:
        print(f"ERROR in get_open_shifts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

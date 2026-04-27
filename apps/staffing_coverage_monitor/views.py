from __future__ import annotations
import os
from typing import Any
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from datetime import datetime

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

    missing = [env_name for env_name, value in (("DB_HOST", host), ("DB_USER", user), ("DB_PASSWORD", password)) if not value]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing required database environment variables: {', '.join(missing)}")

    return URL.create(drivername="mysql+pymysql", username=user, password=password, host=host, port=port, database=name)

def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

@router.get("/", response_class=HTMLResponse)
async def staffing_coverage_monitor_index(request: Request):
    return templates.TemplateResponse("apps/staffing_coverage_monitor.html", {"request": request})

@router.get("/data")
async def get_coverage_data():
    engine = _engine()
    try:
        # 1. Fetch all future open shifts
        current_date = datetime.now().strftime('%Y-%m-%d')
        shifts_sql = text("""
            SELECT 
                co.name AS county_name,
                co.id AS county_id,
                p.description AS position_name,
                p.position_id,
                cl.name AS client_name,
                SUM(sp.count - COALESCE(se_counts.taken, 0)) AS open_spots
            FROM event e
            JOIN client cl ON e.client_id = cl.client_id
            JOIN county co ON e.county_id = co.id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            JOIN position p ON sp.position_id = p.position_id
            LEFT JOIN (
                SELECT shift_position_id, COUNT(shift_employee_id) as taken
                FROM shift_employee
                WHERE cancel_reason = 0
                GROUP BY shift_position_id
            ) se_counts ON sp.shift_position_id = se_counts.shift_position_id
            WHERE e.date >= :current_date
              AND e.deleted_at IS NULL 
              AND s.deleted_at IS NULL
              AND sp.was_published != 0
            GROUP BY co.id, p.position_id, cl.client_id
            HAVING open_spots > 0
            ORDER BY co.name, p.description
        """)

        with engine.connect() as connection:
            shifts_df = pd.read_sql(shifts_sql, connection, params={"current_date": current_date})
            
            if shifts_df.empty:
                return JSONResponse(content={"counties": []})

            # Calculate total open spots and collect unique clients per (county, position)
            # Before merging with employee data, we need to collapse the client-level rows
            summary_shifts = shifts_df.groupby(['county_id', 'county_name', 'position_id', 'position_name']).apply(
                lambda x: pd.Series({
                    'open_spots': x['open_spots'].sum(),
                    'client_name': sorted([{"name": row['client_name'], "open_spots": int(row['open_spots'])} for _, row in x.iterrows()], key=lambda k: k['name'])
                })
            ).reset_index()

            # 2. For the positions and counties found, fetch active/eligible employee counts
            county_ids = summary_shifts['county_id'].unique().tolist()
            position_ids = summary_shifts['position_id'].unique().tolist()
            
            emp_sql = text("""
                SELECT 
                    e.county_id,
                    ep.position_id,
                    COUNT(DISTINCT e.employee_id) AS eligible_count
                FROM employee e
                JOIN employee_position ep ON e.employee_id = ep.employee_id
                WHERE e.status = 1 
                  AND ep.eligible = 1
                  AND e.county_id IN :county_ids
                  AND ep.position_id IN :position_ids
                GROUP BY e.county_id, ep.position_id
            """)
            
            emp_df = pd.read_sql(emp_sql, connection, params={
                "county_ids": tuple(county_ids),
                "position_ids": tuple(position_ids)
            })

            # Merge the data
            merged = pd.merge(summary_shifts, emp_df, on=['county_id', 'position_id'], how='left')
            merged['eligible_count'] = merged['eligible_count'].fillna(0).astype(int)

            # Group by county for display
            result = []
            for county_name, group in merged.groupby('county_name'):
                positions = []
                for _, row in group.iterrows():
                    positions.append({
                        "name": row['position_name'],
                        "open_spots": int(row['open_spots']),
                        "eligible_employees": int(row['eligible_count']),
                        "clients": row['client_name']
                    })
                result.append({
                    "county": county_name,
                    "positions": positions
                })

            return JSONResponse(content={"counties": result})

    except Exception as e:
        print(f"ERROR in get_coverage_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

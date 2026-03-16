from __future__ import annotations
import os
from typing import Any
import pandas as pd
from fastapi import APIRouter, HTTPException, Request, Query
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

    missing = [env_name for env_name, value in (("DB_HOST", host), ("DB_USER", user), ("DB_PASSWORD", password)) if not value]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing required database environment variables: {', '.join(missing)}")

    return URL.create(drivername="mysql+pymysql", username=user, password=password, host=host, port=port, database=name)

def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

@router.get("/", response_class=HTMLResponse)
async def client_cancellation_rates_index(request: Request):
    return templates.TemplateResponse("apps/client_cancellation_rates.html", {"request": request})

@router.get("/data")
async def get_cancellation_data(
    start_date: str = Query(..., description="Start Date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End Date (YYYY-MM-DD)")
):
    engine = _engine()
    try:
        # Query 1: Total Shifts Placed per Client
        placed_sql = text("""
            SELECT 
                c.client_id,
                c.name AS client_name,
                SUM(sp.count) AS total_shifts_placed
            FROM event e
            JOIN client c ON e.client_id = c.client_id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY c.client_id, c.name
        """)

        # Query 2: Total Assigned Shifts Cancelled per Client
        cancelled_sql = text("""
            SELECT 
                e.client_id,
                COUNT(se.shift_employee_id) AS total_client_cancellations
            FROM event e
            JOIN shift_employee se ON e.event_id = se.event_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND se.cancel_reason IN (4, 5, 41, 51)
            GROUP BY e.client_id
        """)

        with engine.connect() as connection:
            placed_df = pd.read_sql(placed_sql, connection, params={"start_date": start_date, "end_date": end_date})
            cancelled_df = pd.read_sql(cancelled_sql, connection, params={"start_date": start_date, "end_date": end_date})

        if placed_df.empty:
            return JSONResponse(content={"data": []})

        # Merge the two dataframes on client_id
        if not cancelled_df.empty:
            merged_df = pd.merge(placed_df, cancelled_df, on="client_id", how="left")
            merged_df["total_client_cancellations"] = merged_df["total_client_cancellations"].fillna(0).astype(int)
        else:
            merged_df = placed_df.copy()
            merged_df["total_client_cancellations"] = 0

        # Calculate percentage
        merged_df["total_shifts_placed"] = merged_df["total_shifts_placed"].fillna(0).astype(int)
        
        # Avoid division by zero
        def calc_percentage(row):
            if row["total_shifts_placed"] > 0:
                return round((row["total_client_cancellations"] / row["total_shifts_placed"]) * 100, 2)
            else:
                return 0.0

        merged_df["cancellation_percentage"] = merged_df.apply(calc_percentage, axis=1)

        # Sort by most cancelled shifts
        merged_df = merged_df.sort_values(by=["total_client_cancellations", "cancellation_percentage"], ascending=[False, False])

        # Convert to dictionary for JSON response
        result = merged_df[["client_id", "client_name", "total_shifts_placed", "total_client_cancellations", "cancellation_percentage"]].to_dict(orient="records")

        return JSONResponse(content={"data": result})

    except Exception as e:
        print(f"ERROR in get_cancellation_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

@router.get("/clients")
async def get_clients():
    engine = _engine()
    try:
        sql = text("""
            SELECT client_id, name 
            FROM client 
            WHERE deleted_at IS NULL 
            ORDER BY name ASC
        """)
        with engine.connect() as connection:
            df = pd.read_sql(sql, connection)
        result = df.to_dict(orient="records")
        return JSONResponse(content={"clients": result})
    except Exception as e:
        print(f"ERROR in get_clients: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

@router.get("/graph-data")
async def get_graph_data(
    client_id: int = Query(..., description="Client ID"),
    start_date: str = Query(..., description="Start Date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End Date (YYYY-MM-DD)")
):
    engine = _engine()
    try:
        # Query 1: Total Shifts Placed per Day for this client
        placed_sql = text("""
            SELECT 
                DATE(e.date) AS event_date,
                SUM(sp.count) AS total_shifts_placed
            FROM event e
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            WHERE e.client_id = :client_id
              AND e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY DATE(e.date)
        """)

        # Query 2: Total Assigned Shifts Cancelled per Day for this client
        cancelled_sql = text("""
            SELECT 
                DATE(e.date) AS event_date,
                COUNT(se.shift_employee_id) AS total_client_cancellations
            FROM event e
            JOIN shift_employee se ON e.event_id = se.event_id
            WHERE e.client_id = :client_id
              AND e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND se.cancel_reason IN (4, 5, 41, 51)
            GROUP BY DATE(e.date)
        """)

        with engine.connect() as connection:
            placed_df = pd.read_sql(placed_sql, connection, params={"client_id": client_id, "start_date": start_date, "end_date": end_date})
            cancelled_df = pd.read_sql(cancelled_sql, connection, params={"client_id": client_id, "start_date": start_date, "end_date": end_date})

        if placed_df.empty:
            return JSONResponse(content={"data": []})

        # Ensure event_date is datetime
        placed_df['event_date'] = pd.to_datetime(placed_df['event_date'])
        
        # Merge the two dataframes on event_date
        if not cancelled_df.empty:
            cancelled_df['event_date'] = pd.to_datetime(cancelled_df['event_date'])
            merged_df = pd.merge(placed_df, cancelled_df, on="event_date", how="left")
            merged_df["total_client_cancellations"] = merged_df["total_client_cancellations"].fillna(0).astype(int)
        else:
            merged_df = placed_df.copy()
            merged_df["total_client_cancellations"] = 0

        merged_df["total_shifts_placed"] = merged_df["total_shifts_placed"].fillna(0).astype(int)
        
        # Group by week (Monday to Sunday)
        # Resample on event_date. W-MON with closed='left', label='left' gives Mon-Sun weeks labeled with Monday's date.
        weekly_df = merged_df.set_index('event_date').resample('W-MON', closed='left', label='left').sum().reset_index()

        # Format date back to string for the json response
        weekly_df['week_of'] = weekly_df['event_date'].dt.strftime('%Y-%m-%d')
        
        def calc_percentage(row):
            if row["total_shifts_placed"] > 0:
                return round((row["total_client_cancellations"] / row["total_shifts_placed"]) * 100, 2)
            else:
                return 0.0

        weekly_df["cancellation_percentage"] = weekly_df.apply(calc_percentage, axis=1)

        result = weekly_df[["week_of", "total_shifts_placed", "total_client_cancellations", "cancellation_percentage"]].to_dict(orient="records")

        return JSONResponse(content={"data": result})

    except Exception as e:
        print(f"ERROR in get_graph_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

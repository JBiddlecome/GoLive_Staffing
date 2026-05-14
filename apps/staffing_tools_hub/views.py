import os
import sqlite3

from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from apps.client_drop_off_v2.app import get_drop_off_data

router = APIRouter()

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

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

@router.get("/dashboard-summary", response_class=JSONResponse)
async def get_dashboard_summary(request: Request):
    user = request.session.get("user")
    logged_in_email = user.get("email") if user else None
    
    engine = _engine()
    try:
        with engine.connect() as conn:
            # Check if viewer is a staffing manager
            is_staffing_manager = False
            manager_name = None
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
                    is_staffing_manager = True
                    manager_name = res[0]


            # 1. Staffing Manager Dashboard Data (Clients with fill rate < 90%)
            # Filter by staffing manager if is_staffing_manager is True
            manager_filter = "AND u.email = :email" if is_staffing_manager else ""
            
            metrics_sql = text(f"""
                SELECT
                    c.client_id,
                    c.name AS client_name,
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
                  {manager_filter}
                GROUP BY c.client_id, c.name
            """)
            
            params = {"email": logged_in_email} if is_staffing_manager else {}
            df = pd.read_sql(metrics_sql, conn, params=params)
            
            section_1_records = []
            if not df.empty:
                df['fill_rate'] = (df['filled_shifts'] / df['total_shifts'] * 100).fillna(0).round(1)
                # Filter < 90%
                under_90 = df[df['fill_rate'] < 90.0].copy()
                # Sort by lowest fill rate
                under_90 = under_90.sort_values(by='fill_rate', ascending=True)
                under_90 = under_90.fillna(0)
                section_1_records = under_90.to_dict(orient='records')
                
            # 2. Shift Risk Dashboard Data (Critical category only)
            # We'll use a modified version of the Shift Risk query to filter for Critical status
            # Critical status is defined as now >= t_deadline (24h before shift) or hours_until_deadline <= 24
            risk_sql = text(f"""
                SELECT
                    c.name AS client_name,
                    e.date AS event_date,
                    s.start AS shift_start,
                    sp.count AS total_spots,
                    COALESCE(se_counts.filled, 0) AS filled_spots
                FROM event e
                JOIN client c ON c.client_id = e.client_id
                JOIN venue v ON v.venue_id = e.venue_id
                LEFT JOIN user u ON u.id = v.staffing_manager_id
                JOIN shift s ON s.event_id = e.event_id
                JOIN shift_position sp ON sp.shift_id = s.shift_id
                LEFT JOIN (
                    SELECT shift_position_id, COUNT(shift_employee_id) AS filled
                    FROM shift_employee
                    WHERE cancel_reason = 0 AND deleted_at IS NULL
                    GROUP BY shift_position_id
                ) se_counts ON se_counts.shift_position_id = sp.shift_position_id
                WHERE e.date >= CURDATE()
                  AND e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND sp.deleted_at IS NULL
                  AND sp.was_published != 0
                  AND (sp.count - COALESCE(se_counts.filled, 0)) > 0
                  {manager_filter}
            """)
            
            risk_df = pd.read_sql(risk_sql, conn, params=params)
            section_2_records = []
            
            if not risk_df.empty:
                # Calculate Critical status in Python to match the algorithm in shift_risk_dashboard
                # T_deadline = shift.start - 24 hours
                from datetime import datetime, timedelta
                now = datetime.utcnow()
                
                def is_critical(row):
                    shift_start = pd.to_datetime(row['shift_start'])
                    event_date = pd.to_datetime(row['event_date']).date()
                    today = now.date()
                    
                    # USER REQUEST: Only Future shifts (strictly > today)
                    if event_date <= today:
                        return False
                        
                    t_deadline = shift_start - timedelta(hours=24)
                    
                    remaining_hours = (t_deadline - now).total_seconds() / 3600
                    return now >= t_deadline or remaining_hours <= 24

                risk_df['is_critical'] = risk_df.apply(is_critical, axis=1)
                critical_df = risk_df[risk_df['is_critical']].copy()

                
                if not critical_df.empty:
                    # Group by client to aggregate risk data
                    critical_df['open_spots'] = critical_df['total_spots'] - critical_df['filled_spots']
                    critical_df['event_date'] = pd.to_datetime(critical_df['event_date']).dt.date
                    today = now.date()
                    
                    summary_df = critical_df.groupby('client_name').agg({
                        'event_date': lambda x: (x.min() - today).days, # Number of days (until soonest critical event)
                        'open_spots': 'sum',
                        'total_spots': 'sum'
                    }).reset_index()

                    
                    # Calculate percentage filled
                    summary_df['fill_pct'] = ( (summary_df['total_spots'] - summary_df['open_spots']) / summary_df['total_spots'] * 100).round(1)
                    summary_df = summary_df.rename(columns={'event_date': 'days_until'})
                    
                    section_2_records = summary_df[['client_name', 'days_until', 'open_spots', 'fill_pct']].to_dict(orient='records')

            # 4. Unconfirmed Requests Data
            from datetime import datetime, timedelta
            from zoneinfo import ZoneInfo
            
            now_la = datetime.now(ZoneInfo("America/Los_Angeles")).replace(tzinfo=None)
            today_str = now_la.date().isoformat()
            six_months_later_str = (now_la.date() + timedelta(days=180)).isoformat()
            
            unconfirmed_sql = text(f"""
                SELECT
                    se.created_at,
                    CONCAT(emp.first_name, ' ', emp.last_name) as employee_name,
                    e.date as event_date,
                    c.name as client_name,
                    c.bundle,
                    v.name as venue_name
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
                  {("AND u_mgr.email = :email" if is_staffing_manager else "")}
            """)
            
            unconf_params = {"start_date": today_str, "end_date": six_months_later_str}
            if is_staffing_manager:
                unconf_params["email"] = logged_in_email
                
            unconf_df = pd.read_sql(unconfirmed_sql, conn, params=unconf_params)
            section_4_records = []
            
            if not unconf_df.empty:
                unconf_df['hours_passed'] = unconf_df['created_at'].apply(
                    lambda x: round((now_la - x).total_seconds() / 3600, 1) if pd.notnull(x) else 0
                )
                unconf_df = unconf_df.sort_values(by='hours_passed', ascending=False)
                unconf_df['created_at'] = unconf_df['created_at'].astype(str)
                unconf_df['event_date'] = unconf_df['event_date'].astype(str)
                unconf_df = unconf_df.fillna("")
                section_4_records = unconf_df.to_dict(orient='records')

            return JSONResponse({
                "status": "success",
                "viewer": {
                    "email": logged_in_email,
                    "is_staffing_manager": is_staffing_manager
                },
                "section_1": section_1_records,
                "section_2": section_2_records,
                "section_3": _get_client_drop_off_summary(is_staffing_manager, manager_name),
                "section_4": section_4_records
            })

            
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

def _get_client_drop_off_summary(is_staffing_manager: bool, manager_name: str | None) -> List[Dict[str, Any]]:
    try:
        # 1. Get raw data from MariaDB logic
        all_clients = get_drop_off_data()
        
        # 2. Filter by staffing manager if applicable
        if is_staffing_manager and manager_name:
            all_clients = [c for c in all_clients if c.get("Staffing Manager") == manager_name]

        # 3. Load local state (Notes and Contacted) from SQLite
        db_path = Path("data/client_drop_off.db")
        if not db_path.exists():
            # Fallback to v2 if that's what's used
            db_path = Path("data/client_drop_off_v2.db")
            
        notes_map = {}
        contacted_map = {}
        
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                # Latest note for each client
                note_rows = conn.execute("""
                    SELECT client_key, note_text, MAX(created_date) as last_date
                    FROM client_drop_off_notes
                    GROUP BY client_key
                """).fetchall()

                notes_map = {row["client_key"]: row["note_text"] for row in note_rows}
                
                # Active contacted/ignored status
                from datetime import date
                today = date.today().isoformat()
                contacted_rows = conn.execute("""
                    SELECT client_key, ignore_until
                    FROM client_drop_off_contacted
                    WHERE ignore_until > ?
                """, (today,)).fetchall()
                contacted_map = {row["client_key"]: row["ignore_until"] for row in contacted_rows}

        # 4. Merge and Filter
        results = []
        for c in all_clients:
            # Filtering: If manager view, we check if the manager name matches or if the email matches if we could join.
            # For now, we'll assume get_drop_off_data is the source of truth for "Staffing Manager" name.
            # We already have manager filter in the Hub API for sections 1 & 2. 
            # Section 3 should follow suit.
            
            # Since get_drop_off_data doesn't take email, we filter results manually if needed.
            # But wait, get_drop_off_data returns "Staffing Manager" name.
            # We should probably pass the manager name to this helper.
            
            # For simplicity, we'll return all for now and ensure manager name matches if we can find it.
            client_name = c.get("Client")
            c_manager = c.get("Staffing Manager")

            if not client_name:
                continue
                
            client_key = client_name.strip().lower()
            
            # Skip if ignored
            if client_key in contacted_map:
                continue
                
            results.append({
                "client_name": client_name,
                "days_since": c.get("Days Since Last Shift", 0),
                "last_note": notes_map.get(client_key, ""),
                "staffing_manager": c_manager
            })
            
        return results[:20] # Top 20 for the block
    except Exception as e:
        print(f"Error in drop off summary: {e}")
        return []

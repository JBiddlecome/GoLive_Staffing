from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os
import pandas as pd
from datetime import datetime, timedelta

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

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

def _ensure_state_table():
    engine = _engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS golive_app_state (
                    app_name VARCHAR(100) NOT NULL,
                    state_key VARCHAR(100) NOT NULL,
                    state_value LONGTEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (app_name, state_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """))
    except Exception as e:
        print(f"[Client Notifications] Error ensuring state table: {e}")
    finally:
        engine.dispose()

def _get_setting(key, default=None):
    engine = _engine()
    try:
        with engine.begin() as conn:
            res = conn.execute(text("SELECT state_value FROM golive_app_state WHERE app_name = 'client_notifications' AND state_key = :key"), {"key": key}).fetchone()
            if res:
                val = res[0]
                if val.lower() == 'true': return True
                if val.lower() == 'false': return False
                return val
            return default
    except Exception:
        return default
    finally:
        engine.dispose()

def _set_setting(key, value):
    engine = _engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO golive_app_state (app_name, state_key, state_value)
                VALUES ('client_notifications', :key, :value)
                ON DUPLICATE KEY UPDATE state_value = :value
            """), {"key": key, "value": str(value)})
    except Exception as e:
        print(f"[Client Notifications] Error setting setting: {e}")
    finally:
        engine.dispose()

async def get_prior_month_data(client_id: int):
    # Calculate prior month range
    today = datetime.now()
    first_of_this_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_date = first_of_this_month - timedelta(seconds=1)
    start_date = (first_of_this_month - timedelta(days=1)).replace(day=1)
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    start_date_only = start_date.strftime("%Y-%m-%d")
    end_date_only = end_date.strftime("%Y-%m-%d")

    engine = _engine()
    data = {}
    try:
        with engine.begin() as conn:
            # 1. Preferred Employees added
            sql_preferred = text("""
                SELECT COUNT(*) 
                FROM exclusive 
                WHERE client_id = :client_id 
                  AND date_created BETWEEN :start AND :end
            """)
            data['preferred_added_count'] = int(conn.execute(sql_preferred, {"client_id": client_id, "start": start_str, "end": end_str}).scalar() or 0)

            # 2. Worked Employees (Non-DNR), Positions, Hours
            sql_worked = text("""
                SELECT 
                    p.description AS position,
                    SUM(CASE 
                        WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds
                        WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds
                        ELSE t.client_seconds
                    END) / 3600.0 AS hours,
                    COUNT(DISTINCT t.employee_id) AS employee_count,
                    AVG(NULLIF(t.employee_rating, 0)) AS avg_rating
                FROM timesheet t
                JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN position p ON sp.position_id = p.position_id
                JOIN event e ON t.event_id = e.event_id
                LEFT JOIN dnr d ON t.employee_id = d.employee_id AND d.client_id = :client_id
                WHERE e.client_id = :client_id
                  AND e.date BETWEEN :start_date AND :end_date
                  AND t.employee_worked IN ('WORKED', 'SENTHOME')
                  AND d.employee_id IS NULL
                GROUP BY p.description
            """)
            worked_df = pd.read_sql(sql_worked, conn, params={
                "client_id": client_id, 
                "start_date": start_date_only, 
                "end_date": end_date_only
            })
            
            positions = []
            for rec in worked_df.to_dict(orient="records"):
                clean_rec = {}
                for k, v in rec.items():
                    clean_rec[k] = None if pd.isna(v) else v
                positions.append(clean_rec)
            data['positions'] = positions

            total_hours = worked_df['hours'].sum() if not worked_df.empty else 0
            data['total_hours'] = float(total_hours) if pd.notna(total_hours) else 0.0

            avg_rating = worked_df['avg_rating'].mean() if not worked_df.empty else None
            data['avg_rating'] = float(avg_rating) if pd.notna(avg_rating) else None

            # 3. List of Employees
            sql_employee_list = text("""
                SELECT DISTINCT CONCAT(emp.first_name, ' ', emp.last_name) as name
                FROM timesheet t
                JOIN employee emp ON t.employee_id = emp.employee_id
                JOIN event e ON t.event_id = e.event_id
                LEFT JOIN dnr d ON t.employee_id = d.employee_id AND d.client_id = :client_id
                WHERE e.client_id = :client_id
                  AND e.date BETWEEN :start_date AND :end_date
                  AND t.employee_worked IN ('WORKED', 'SENTHOME')
                  AND d.employee_id IS NULL
                ORDER BY emp.first_name, emp.last_name
            """)
            emp_list_df = pd.read_sql(sql_employee_list, conn, params={
                "client_id": client_id, 
                "start_date": start_date_only, 
                "end_date": end_date_only
            })
            data['employees_worked'] = emp_list_df['name'].tolist()

            # 4. Avg Time before creating and filling
            sql_timing = text("""
                SELECT 
                    AVG(TIMESTAMPDIFF(SECOND, sp.created_at, s.start)) as avg_creation_to_start_seconds,
                    AVG(TIMESTAMPDIFF(SECOND, sp.created_at, se.confirmed_at)) as avg_creation_to_fill_seconds
                FROM shift_position sp
                JOIN shift s ON sp.shift_id = s.shift_id
                JOIN event e ON s.event_id = e.event_id
                LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.confirmed = 1 AND se.deleted_at IS NULL
                WHERE e.client_id = :client_id
                  AND e.date BETWEEN :start_date AND :end_date
                  AND sp.deleted_at IS NULL
            """)
            timing = conn.execute(sql_timing, {
                "client_id": client_id, 
                "start_date": start_date_only, 
                "end_date": end_date_only
            }).fetchone()
            
            data['avg_creation_to_start_hours'] = float(timing[0]) / 3600.0 if timing and pd.notna(timing[0]) else None
            data['avg_creation_to_fill_hours'] = float(timing[1]) / 3600.0 if timing and pd.notna(timing[1]) else None

            # 5. Fill Rates (>24h vs <24h)
            sql_fill_rates = text("""
                SELECT 
                    timing_group,
                    SUM(needed) as total_needed,
                    SUM(LEAST(needed, actual_filled)) as total_filled
                FROM (
                    SELECT 
                        CASE WHEN TIMESTAMPDIFF(HOUR, sp.created_at, s.start) > 24 THEN 'over_24h' ELSE 'under_24h' END as timing_group,
                        sp.count as needed,
                        (
                            SELECT COUNT(*) 
                            FROM shift_employee se 
                            WHERE se.shift_position_id = sp.shift_position_id 
                              AND se.deleted_at IS NULL 
                              AND se.cancel_reason = 0 
                              AND se.confirmed = 1
                        ) as actual_filled
                    FROM shift_position sp
                    JOIN shift s ON sp.shift_id = s.shift_id
                    JOIN event e ON s.event_id = e.event_id
                    WHERE e.client_id = :client_id
                      AND e.date BETWEEN :start_date AND :end_date
                      AND sp.deleted_at IS NULL
                ) sub
                GROUP BY timing_group
            """)
            fill_df = pd.read_sql(sql_fill_rates, conn, params={
                "client_id": client_id, 
                "start_date": start_date_only, 
                "end_date": end_date_only
            })
            
            fill_stats = fill_df.set_index('timing_group').to_dict(orient="index")
            
            def get_rate(group_name):
                group = fill_stats.get(group_name)
                if not group: return None
                needed = group.get('total_needed', 0)
                if pd.isna(needed) or needed == 0: return 0.0
                filled = group.get('total_filled', 0)
                filled = 0.0 if pd.isna(filled) else float(filled)
                return float((filled / float(needed)) * 100.0)

            data['fill_rate_over_24h'] = get_rate('over_24h')
            data['fill_rate_under_24h'] = get_rate('under_24h')

    finally:
        engine.dispose()
    
    return data

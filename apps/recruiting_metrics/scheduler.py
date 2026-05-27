import asyncio
import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text
from apps.recruiting_metrics.views import _engine

LA_TZ = ZoneInfo("America/Los_Angeles")
db_path = "db.sqlite3"

def run_weekly_recruiting_update():
    now = datetime.now(LA_TZ)
    
    # We only run the actual calculations on Monday
    if now.weekday() != 0:
        return
        
    prev_sunday = now - timedelta(days=1)
    prev_sunday_str = prev_sunday.strftime("%Y-%m-%d")
    
    # Connect to SQLite to check if already done
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT worked FROM active_staff_trends WHERE date = ?;", (prev_sunday_str,))
    row = cursor.fetchone()
    
    # If the worked column is already populated, we skip to avoid duplicate runs
    if row and row[0] is not None:
        conn.close()
        return
        
    print(f"[Recruiting Metrics Scheduler] Running weekly update for week ending {prev_sunday_str}...")
    
    # 1. Fetch current active employee count from MySQL
    engine = _engine()
    active_count = 0
    try:
        with engine.connect() as mysql_conn:
            active_sql = text("SELECT COUNT(*) FROM employee WHERE status = 1 AND deleted_at IS NULL;")
            active_count = mysql_conn.execute(active_sql).scalar() or 0
    except Exception as e:
        print(f"[Recruiting Metrics Scheduler] Error fetching active employee count: {e}")
        engine.dispose()
        conn.close()
        return
        
    # 2. Fetch unique employees worked count for the week from MySQL
    worked_count = 0
    start_date_str = (prev_sunday - timedelta(days=6)).strftime("%Y-%m-%d")
    end_date_str = prev_sunday_str
    
    try:
        with engine.connect() as mysql_conn:
            worked_sql = text("""
                SELECT COUNT(DISTINCT t.employee_id)
                FROM timesheet t
                JOIN event e ON t.event_id = e.event_id
                WHERE e.date >= :start_date
                  AND e.date <= :end_date
                  AND e.deleted_at IS NULL
                  AND (
                      (t.employee_seconds IS NOT NULL AND t.employee_seconds > 0)
                      OR (t.client_seconds IS NOT NULL AND t.client_seconds > 0)
                      OR t.client_min_bill = 1
                      OR t.employee_min_pay = 1
                  );
            """)
            worked_count = mysql_conn.execute(worked_sql, {
                "start_date": start_date_str,
                "end_date": end_date_str
            }).scalar() or 0
    except Exception as e:
        print(f"[Recruiting Metrics Scheduler] Error fetching worked employee count: {e}")
        engine.dispose()
        conn.close()
        return
    finally:
        engine.dispose()
        
    # 3. Retrieve or fallback active staff count for the previous Sunday
    cursor.execute("SELECT active_staff FROM active_staff_trends WHERE date = ?;", (prev_sunday_str,))
    active_staff_row = cursor.fetchone()
    if active_staff_row and active_staff_row[0] is not None:
        active_staff_count = active_staff_row[0]
    else:
        # Fallback to current active count if record didn't exist
        active_staff_count = active_count
        
    # Calculate percentage
    percent_working = 0.0
    if active_staff_count > 0:
        percent_working = round((worked_count / active_staff_count) * 100, 2)
        
    # 4. Update SQLite for the previous Sunday
    cursor.execute("""
        INSERT INTO active_staff_trends (date, active_staff, worked, percent_working)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            worked = excluded.worked,
            percent_working = excluded.percent_working,
            active_staff = excluded.active_staff;
    """, (prev_sunday_str, active_staff_count, worked_count, percent_working))
    
    # 5. Add Active Staff count for the UPCOMING Sunday (prev_sunday + 7 days)
    upcoming_sunday = prev_sunday + timedelta(days=7)
    upcoming_sunday_str = upcoming_sunday.strftime("%Y-%m-%d")
    
    cursor.execute("""
        INSERT INTO active_staff_trends (date, active_staff)
        VALUES (?, ?)
        ON CONFLICT(date) DO UPDATE SET
            active_staff = excluded.active_staff;
    """, (upcoming_sunday_str, active_count))
    
    conn.commit()
    conn.close()
    print(f"[Recruiting Metrics Scheduler] Completed weekly update for week ending {prev_sunday_str}. "
          f"Active: {active_staff_count}, Worked: {worked_count}, Percent Working: {percent_working}%. "
          f"Upcoming active staff count seeded for {upcoming_sunday_str} as {active_count}.")

async def recruiting_metrics_monitoring_loop():
    # Random offset to avoid running right as server starts heavily
    await asyncio.sleep(45)
    
    print("[Recruiting Metrics Scheduler] Active Staffing Trends background monitor loop ACTIVE.")
    
    while True:
        try:
            run_weekly_recruiting_update()
        except Exception as error:
            print(f"[Recruiting Metrics Scheduler] Exception cycle interrupt: {error}")
            
        # Check once every 4 hours
        await asyncio.sleep(14400)

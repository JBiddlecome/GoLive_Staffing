import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import json
import logging
from openai import OpenAI, OpenAIError
from datetime import datetime, date, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

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
async def daily_report_assessment_page(request: Request):
    return templates.TemplateResponse("apps/daily_report_assessment.html", {"request": request})

@router.get("/api/reports", response_class=JSONResponse)
async def get_daily_reports(date_str: str = Query(..., alias="date")):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                en.employee_note_id,
                e.employee_id, 
                e.first_name, 
                e.last_name, 
                e.email,
                e.mobile,
                en.note as daily_report_notes,
                c.name as client_name,
                v.name as venue_name,
                p.description as position_name,
                en.datetime as start
            FROM employee_note en
            JOIN employee e ON en.employee_id = e.employee_id
            LEFT JOIN shift_employee se ON en.shift_employee_id = se.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN shift s ON sp.shift_id = s.shift_id
            LEFT JOIN event ev ON s.event_id = ev.event_id
            LEFT JOIN client c ON ev.client_id = c.client_id
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            WHERE DATE(en.datetime) = :date
              AND (en.type LIKE '%DAILY%' OR en.type LIKE '%HR_DAILY%')
        """)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"date": date_str})
        
        # Fill NaN values to prevent JSON serialization errors
        df = df.fillna("")
        for col in ['start']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) and x != "" else "")

        return JSONResponse({"status": "success", "data": df.to_dict(orient="records")})
    except Exception as e:
        logger.exception("Failed to query daily reports")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/api/assessment/{employee_id}", response_class=JSONResponse)
async def get_assessment_data(employee_id: int, employee_note_id: int):
    api_key = os.getenv("DAILY_REPORT_ASSESSMENT") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse({"status": "error", "message": "DAILY_REPORT_ASSESSMENT or OPENAI_API_KEY is not configured."}, status_code=500)
        
    engine = _engine()
    try:
        with engine.connect() as conn:
            # 1. Header Info (same as employee dashboard)
            header_sql = text("""
                SELECT 
                    e.employee_id, e.first_name, e.last_name, e.email, e.mobile, e.status, 
                    e.region, c.name as county_name, e.start_date,
                    (SELECT start FROM shift JOIN shift_position USING(shift_id) JOIN shift_employee USING(shift_position_id) JOIN timesheet t USING(shift_employee_id) WHERE t.employee_id = e.employee_id AND t.employee_worked = 'WORKED' ORDER BY start DESC LIMIT 1) as last_worked_date
                FROM employee e
                LEFT JOIN county c ON e.county_id = c.id
                WHERE e.employee_id = :emp_id
            """)
            header_df = pd.read_sql(header_sql, conn, params={"emp_id": employee_id})
            if header_df.empty:
                return JSONResponse({"status": "error", "message": "Employee not found"}, status_code=404)
            
            header_data = header_df.to_dict(orient="records")[0]
            if pd.notnull(header_data.get('start_date')):
                header_data['start_date'] = header_data['start_date'].isoformat()
            else:
                header_data['start_date'] = ""
                
            if pd.notnull(header_data.get('last_worked_date')):
                header_data['last_worked_date'] = header_data['last_worked_date'].isoformat()
            else:
                header_data['last_worked_date'] = ""
                
            for k, v in header_data.items():
                if pd.isna(v):
                    header_data[k] = ""

            # 2. KPIs
            # Worked Shifts
            worked_sql = text("""
                SELECT COUNT(DISTINCT t.timesheet_id) as worked_shifts 
                FROM timesheet t
                JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                WHERE t.employee_id = :emp_id 
                  AND t.employee_worked = 'WORKED'
                  AND s.start <= NOW()
            """)
            worked_count = conn.execute(worked_sql, {"emp_id": employee_id}).scalar() or 0

            # Cancelled Shifts (2='< 24 Hours Notice', 3='> 24 Hours Notice')
            cancelled_sql = text("""
                SELECT COUNT(*) as cancelled_shifts, SUM(CASE WHEN se.cancel_reason = 2 THEN 1 ELSE 0 END) as cancel_under_24
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                WHERE se.employee_id = :emp_id
                  AND se.cancel_reason IN (2, 3)
                  AND s.start <= NOW()
            """)
            cancel_row = conn.execute(cancelled_sql, {"emp_id": employee_id}).fetchone()
            cancelled_count = cancel_row[0] if cancel_row else 0
            cancel_under_24 = cancel_row[1] if cancel_row and cancel_row[1] else 0

            # Cancel < 24 reasons
            cancel_24_reasons_sql = text("""
                SELECT cancel_notes FROM shift_employee 
                WHERE employee_id = :emp_id AND cancel_reason = 2 AND cancel_notes IS NOT NULL AND cancel_notes != ''
            """)
            cancel_24_rs = conn.execute(cancel_24_reasons_sql, {"emp_id": employee_id}).fetchall()
            cancel_under_24_reasons_list = [r[0] for r in cancel_24_rs]

            # No-Shows
            noshow_sql = text("""
                SELECT COUNT(DISTINCT t.timesheet_id) as noshow_shifts 
                FROM timesheet t
                JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                WHERE t.employee_id = :emp_id 
                  AND t.employee_worked = 'NOSHOW'
                  AND s.start <= NOW()
            """)
            noshow_count = conn.execute(noshow_sql, {"emp_id": employee_id}).scalar() or 0

            # Avg Clock-in Variance
            variance_sql = text("""
                SELECT AVG(TIMESTAMPDIFF(MINUTE, s.start, t.employee_start)) as avg_variance
                FROM timesheet t
                JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
                JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
                JOIN shift s ON s.shift_id = sp.shift_id
                WHERE t.employee_id = :emp_id
                  AND t.employee_start IS NOT NULL
                  AND t.employee_worked = 'WORKED'
                  AND s.start <= NOW()
            """)
            variance_row = conn.execute(variance_sql, {"emp_id": employee_id}).fetchone()
            avg_variance = round(float(variance_row[0]), 1) if variance_row and variance_row[0] is not None else None

            # Preferred Counts
            pref_sql = text("""
                SELECT COUNT(DISTINCT client_id) AS pref_clients
                FROM exclusive WHERE employee_id = :emp_id
            """)
            pref_row = conn.execute(pref_sql, {"emp_id": employee_id}).fetchone()
            pref_clients = pref_row[0] if pref_row else 0

            # DNR Counts and Reasons
            dnr_sql = text("""
                SELECT d.dnr_id, sr.reason, d.other_reason, d.notes
                FROM dnr d
                LEFT JOIN status_reason sr ON d.reason_id = sr.id
                WHERE d.employee_id = :emp_id
            """)
            dnr_rs = conn.execute(dnr_sql, {"emp_id": employee_id}).fetchall()
            dnr_clients = len(dnr_rs)
            dnr_reasons_list = []
            for r in dnr_rs:
                rsn = r[1] or r[2] or "Unknown reason"
                note = r[3] or "No notes"
                dnr_reasons_list.append(f"{rsn}: {note}")

            # Avg Confirmation Time
            conf_time_sql = text("""
                SELECT AVG(TIMESTAMPDIFF(MINUTE, se.created_at, se.confirmed_at)) as avg_conf_time
                FROM shift_employee se
                JOIN user u ON se.request_by = u.id
                WHERE se.employee_id = :emp_id
                  AND u.`group` IN ('ADMIN', 'OWNER')
                  AND se.confirmed = 1
                  AND se.cancel_reason = 0
            """)
            conf_time_val = conn.execute(conf_time_sql, {"emp_id": employee_id}).scalar()
            avg_conf_time = round(float(conf_time_val), 1) if conf_time_val is not None else None

            # 3. Employee Notes
            notes_sql = text("""
                SELECT type, datetime, note
                FROM employee_note
                WHERE employee_id = :emp_id AND datetime >= DATE_SUB(NOW(), INTERVAL 3 YEAR)
                ORDER BY datetime DESC
            """)
            notes_rs = conn.execute(notes_sql, {"emp_id": employee_id}).fetchall()
            all_notes_list = []
            for row in notes_rs:
                n_type = row[0]
                n_dt = row[1]
                n_note = row[2]
                if n_note and n_note.strip():
                    dt_str = n_dt.strftime("%Y-%m-%d") if pd.notnull(n_dt) else ""
                    all_notes_list.append(f"[{dt_str}] ({n_type}) {n_note.strip()}")

            # 4. Fetch the specific Daily Report Note
            dr_sql = text("SELECT note FROM employee_note WHERE employee_note_id = :nid")
            dr_row = conn.execute(dr_sql, {"nid": employee_note_id}).fetchone()
            daily_report_note = dr_row[0] if dr_row else "No daily report note found."

            # Optional Shift History (for UI)
            history_sql = text("""
                SELECT 
                    e.date as event_date, 
                    c.name as client_name, 
                    v.name as venue_name, 
                    p.description as position_name, 
                    s.start as scheduled_start,
                    s.end as scheduled_end,
                    t.employee_start,
                    t.employee_end,
                    t.employee_worked
                FROM shift_employee se
                JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
                JOIN position p ON sp.position_id = p.position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                JOIN event e ON s.event_id = e.event_id
                JOIN client c ON e.client_id = c.client_id
                LEFT JOIN venue v ON e.venue_id = v.venue_id
                LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
                WHERE se.employee_id = :emp_id
                  AND e.date <= CURDATE()
                ORDER BY e.date DESC
                LIMIT 40
            """)
            history_df = pd.read_sql(history_sql, conn, params={"emp_id": employee_id})
            history_df = history_df.fillna("")
            for col in ['event_date', 'scheduled_start', 'scheduled_end', 'employee_start', 'employee_end']:
                if col in history_df.columns:
                    history_df[col] = history_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) and x != "" else "")
            shift_history = history_df.to_dict(orient="records")

            # 5. Connect with AI for Assessment
            notes_text = "\n".join(all_notes_list[:50]) # limit to 50 notes
            history_prompt = f"""
Employee History Summary:
- Total Worked Shifts: {worked_count}
- Total Cancelled Shifts: {cancelled_count} (Cancellations under 24hrs notice: {cancel_under_24})
- Cancellation under 24hr reasons: {"; ".join(cancel_under_24_reasons_list[:20])}
- Total No-Shows: {noshow_count}
- Average Clock-in Variance: {avg_variance} mins
- Preferred List Clients: {pref_clients}
- DNR Clients: {dnr_clients}
- DNR Reasons: {"; ".join(dnr_reasons_list)}
- Other Notes (Recent):
{notes_text}
"""

            prompt = f"""You are an HR reviewer analyzing a daily report note for an employee and comparing it against their historical performance data.

**Daily Report Note:**
"{daily_report_note}"

**Employee History Data:**
{history_prompt}

**Tasks:**
1. Determine the category of the daily report note. Choose the best fit from: ["Late Arrival", "Cancellation < 24 Hours", "Issue Working", "DNR/No Show", "Other"].
2. Write an assessment specifically about this daily report note in relation to the employee's history. 
   - If Late Arrival: Highlight their average clock-in variance and any notes of past tardiness.
   - If Cancellation < 24 Hours: Highlight the overall count of <24h cancellations and point out the reasons they previously provided.
   - If Issue Working: Briefly review the other notes to see if similar issues have been reported.
   - If DNR or No Show: Mention the total count and reasons for past DNRs or No Shows.
   - If none match strictly, provide a summary comparing the note to their overall reliability.
   
Return valid JSON only.
Use this exact schema:
{{
  "category": "string",
  "assessment": "string"
}}
"""

            client = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            ai_data = json.loads(response.choices[0].message.content)

            return JSONResponse({
                "status": "success",
                "employee": header_data,
                "daily_report_note": daily_report_note,
                "kpis": {
                    "worked_shifts": worked_count,
                    "cancelled_shifts": cancelled_count,
                    "noshow_shifts": noshow_count,
                    "avg_clock_in_variance_minutes": avg_variance,
                    "preferred_clients": pref_clients,
                    "dnr_clients": dnr_clients,
                    "avg_confirmation_minutes": avg_conf_time
                },
                "shift_history": shift_history,
                "ai_assessment": ai_data
            })
            
    except OpenAIError as e:
        logger.exception("OpenAI API call failed")
        return JSONResponse({"status": "error", "message": "Failed to generation AI summary: " + str(e)}, status_code=500)
    except Exception as e:
        logger.exception("Failed to query assessment data")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

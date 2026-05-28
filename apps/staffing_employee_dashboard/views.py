import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import json
import logging
import re
from openai import OpenAI, OpenAIError

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
async def staffing_employee_dashboard_page(request: Request):
    return templates.TemplateResponse("apps/staffing_employee_dashboard.html", {"request": request})

@router.get("/api/search", response_class=JSONResponse)
async def search_employees(q: str = Query(..., min_length=2)):
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                employee_id, 
                first_name, 
                last_name, 
                email, 
                mobile, 
                status, 
                region 
            FROM employee 
            WHERE status != 'DELETED'
              AND first_name NOT LIKE '%[DELETED]%'
              AND last_name NOT LIKE '%[DELETED]%'
              AND email NOT LIKE '%[DELETED]%'
              AND (first_name LIKE :q OR last_name LIKE :q OR CONCAT(first_name, ' ', last_name) LIKE :q OR email LIKE :q OR mobile LIKE :q)
            LIMIT 20
        """)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"q": f"%{q}%"})
        
        # Fill NaN values to prevent JSON serialization errors
        df = df.fillna("")
        
        return JSONResponse({"status": "success", "data": df.to_dict(orient="records")})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/api/dashboard/{employee_id}/reliability", response_class=JSONResponse)
async def get_dashboard_reliability(request: Request, employee_id: int):
    from apps.api_usage_tracker import log_api_usage
    log_api_usage("Staffing Employee Dashboard", request=request)
    
    api_key = os.getenv("STAFFING_EMPLOYEE_DASHBOARD") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse({"status": "error", "message": "STAFFING_EMPLOYEE_DASHBOARD or OPENAI_API_KEY is not configured."}, status_code=500)
    
    engine = _engine()
    notes = []
    try:
        sql = text("""
            SELECT type, datetime, note
            FROM employee_note
            WHERE employee_id = :emp_id AND (type LIKE '%PERSONNEL%' OR type LIKE '%DAILY%') AND datetime >= DATE_SUB(NOW(), INTERVAL 3 YEAR)
            ORDER BY datetime DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(sql, {"emp_id": employee_id}).fetchall()
            for row in result:
                n_type = row[0]
                n_dt = row[1]
                n_note = row[2]
                if n_note and n_note.strip():
                    dt_str = n_dt.strftime("%Y-%m-%d") if pd.notnull(n_dt) else ""
                    notes.append(f"[{dt_str}] ({n_type}) {n_note.strip()}")
            
            worked_sql = text("SELECT COUNT(DISTINCT t.timesheet_id) FROM timesheet t JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id JOIN shift s ON sp.shift_id = s.shift_id WHERE t.employee_id = :emp_id AND t.employee_worked = 'WORKED' AND s.start <= NOW()")
            total_shifts_worked = conn.execute(worked_sql, {"emp_id": employee_id}).scalar() or 0

            noshow_sql = text("SELECT COUNT(DISTINCT t.timesheet_id) FROM timesheet t JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id JOIN shift s ON sp.shift_id = s.shift_id WHERE t.employee_id = :emp_id AND t.employee_worked = 'NOSHOW' AND s.start <= NOW()")
            confirmed_no_shows = conn.execute(noshow_sql, {"emp_id": employee_id}).scalar() or 0

            dnr_sql = text("SELECT COUNT(*) as cnt, GROUP_CONCAT(COALESCE(sr.reason, d.other_reason) SEPARATOR '; ') as details, GROUP_CONCAT(CASE WHEN d.notes IS NOT NULL AND d.notes != '' THEN d.notes END SEPARATOR '; ') as notes FROM dnr d LEFT JOIN status_reason sr ON d.reason_id = sr.id WHERE d.employee_id = :emp_id")
            dnr_row = conn.execute(dnr_sql, {"emp_id": employee_id}).fetchone()
            dnr_count = dnr_row[0] if dnr_row else 0
            dnr_details = dnr_row[1] if dnr_row and dnr_row[1] else "None"
            dnr_notes = dnr_row[2] if dnr_row and dnr_row[2] else "None"

            pref_sql = text("SELECT COUNT(*) as cnt, GROUP_CONCAT(reason SEPARATOR '; ') as details, GROUP_CONCAT(CASE WHEN notes IS NOT NULL AND notes != '' THEN notes END SEPARATOR '; ') as notes FROM exclusive WHERE employee_id = :emp_id")
            pref_row = conn.execute(pref_sql, {"emp_id": employee_id}).fetchone()
            preferred_count = pref_row[0] if pref_row else 0
            preferred_details = pref_row[1] if pref_row and pref_row[1] else "None"
            preferred_notes = pref_row[2] if pref_row and pref_row[2] else "None"

            variance_sql = text("SELECT AVG(TIMESTAMPDIFF(MINUTE, s.start, t.employee_start)) FROM timesheet t JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id JOIN shift s ON s.shift_id = sp.shift_id WHERE t.employee_id = :emp_id AND t.employee_start IS NOT NULL AND t.employee_worked = 'WORKED' AND s.start <= NOW()")
            variance_val = conn.execute(variance_sql, {"emp_id": employee_id}).scalar()
            clock_in_variance = round(float(variance_val), 1) if variance_val is not None else 0

    except Exception as e:
        logger.exception("Failed to query employee data")
        return JSONResponse({"status": "error", "message": "Database query failed"}, status_code=500)
    finally:
        engine.dispose()
        
    notes_text = "\n".join(notes) if notes else "No notes on file."
    
    prompt = f"""You are analyzing internal employee notes and structured metrics for a staffing manager.
Your task is to produce a balanced and context-aware reliability assessment.
IMPORTANT CONTEXT:
•	Notes are biased toward negative events (issues are logged more often than positive performance).
•	Some notes may include automated text such as: "Worked auto updated to 'No Show'" which does NOT necessarily indicate a true no-show.
•	You will also be given structured metrics (shift counts, confirmed no-shows, DNR counts, etc.) which MUST be used to contextualize the notes.
INPUT DATA:
•	notes_text: freeform notes
•	total_shifts_worked: integer
•	confirmed_no_shows: integer (actual verified no-shows)
•	dnr_count: integer
•	dnr_details: optional text describing reasons for DNR
•	dnr_notes: optional freeform notes associated with DNR entries
•	preferred_count: integer
•	preferred_details: optional text describing reasons for Preferred
•	preferred_notes: optional freeform notes associated with Preferred/Exclusive entries
•	clock_in_variance: integer
ANALYSIS INSTRUCTIONS:
1.	Normalize note bias:
o	Do NOT assume lack of positive notes means poor performance.
o	Absence of praise is neutral, not negative.
2.	Handle "auto updated to No Show" and Cancellations carefully:
o	Treat "auto updated to No Show" as LOW CONFIDENCE signals unless supported by confirmed_no_shows or other notes.
o	If a note says "Employee Cancelled > 24 Hours Notice", it means they met the company requirement. Do NOT lower the reliability score for these notes, unless the quantity of these specific cancellations exceeds 20% of their total_shifts_worked.
3.	Use ratio-based reasoning:
o	Evaluate issues relative to total_shifts_worked.
o	Example: 3 issues over 150 shifts = low concern.
o	Example: 3 issues over 10 shifts = high concern.
4.	Weight categories differently:
o	HIGH severity: confirmed no-call/no-show, walking off job, harassment, aggression, DNR with serious cause, WW or written warning references.
o	MEDIUM severity: repeated lateness, tardiness, warnings, attitude complaints, WW or written warning references.
o	LOW severity: isolated minor issues, vague complaints, auto-generated notes
5.	DNR handling:
o	DNR_count > 1 is a strong negative signal
o	Use dnr_details to determine severity (e.g., misconduct vs minor preference)
6.	Pattern detection:
o	Repeated issues across time are more important than isolated events
o	Recent issues weigh more heavily than older ones
7.	Positive indicators:
o	Only include if explicitly present in notes
o	Do NOT infer positives from absence of negatives
8.	
SCORING METHODOLOGY:
Start from a neutral baseline of 75 and adjust:
Decrease score for:
•	High severity incidents (−15 to −40 each depending on severity, unless isolated and total shifts is very large)
•	Medium severity repeated issues (−5 to −15)
•	DNR events (−15 to −50 depending on reason)
•	High issue-to-shift ratio
Increase score for:
•	Large number of shifts worked with few issues (+5 to +15)
•	Explicit positive feedback (+5 to +15)
•	Long history with minimal escalation
Ratio adjustment (CRITICAL STEP):
•	If total_shifts_worked is extremely high (e.g., > 100) and an issue (even a Written Warning or late cancellation) occurs less than 1-2% of the time, DO NOT heavily penalize the score. The volume of reliable shifts overwhelmingly outweighs an isolated incident. An employee with hundreds of shifts and only a few minor or isolated incidents MUST score 85+.
•	If total_shifts_worked is high (50+), substantially reduce impact of isolated incidents.
•	If total_shifts_worked is low (<20), increase impact of each issue.
Clamp final score between 0 and 100.
SCORE INTERPRETATION:
•	90-100: highly reliable, strong track record relative to volume
•	85-89: highly reliable overall, but has minimal/isolated incidents that are dwarfed by their massive work volume
•	70-84: generally reliable, minor or low-frequency issues
•	40-69: moderate concern, noticeable patterns or ratio concerns
•	0-39: high risk, serious or frequent issues relative to shifts
OUTPUT REQUIREMENTS:
•	Be concise but specific
•	Reference patterns, ratios, and severity (not just raw counts)
•	Clearly distinguish confirmed issues vs ambiguous notes
Return valid JSON only using this schema:
{{
"summary": "string",
"reliability_score": 0,
"risk_factors": ["string"],
"positive_indicators": ["string"],
"notable_incidents": ["string"],
"caution": "string"
}}
Employee notes:
{notes_text}
Structured data:
•	total_shifts_worked: {total_shifts_worked}
•	confirmed_no_shows: {confirmed_no_shows}
•	dnr_count: {dnr_count}
•	dnr_details: {dnr_details}
•	dnr_notes: {dnr_notes}
•	preferred_count: {preferred_count}
•	preferred_details: {preferred_details}
•	preferred_notes: {preferred_notes}
•	clock_in_variance: {clock_in_variance}"""

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        content = response.choices[0].message.content
        data = json.loads(content)
        return JSONResponse({"status": "success", "data": data})
    except OpenAIError as e:
        logger.exception("OpenAI API call failed")
        return JSONResponse({"status": "error", "message": "Failed to generation AI summary: " + str(e)}, status_code=500)
    except json.JSONDecodeError:
        logger.exception("Failed to parse OpenAI JSON response")
        return JSONResponse({"status": "error", "message": "Invalid response format from AI"}, status_code=500)


@router.get("/api/dashboard/{employee_id}", response_class=JSONResponse)
async def get_dashboard_data(employee_id: int):
    engine = _engine()
    try:
        with engine.connect() as conn:
            # Header Info
            header_sql = text("""
                SELECT 
                    e.employee_id, e.first_name, e.last_name, e.email, e.mobile, e.status, 
                    e.region, c.name as county_name, e.start_date,
                    e.work_area_latitude, e.work_area_longitude, e.work_area_distance,
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

            # Eligible Positions
            positions_sql = text("""
                SELECT p.description FROM employee_position ep 
                JOIN position p ON ep.position_id = p.position_id 
                WHERE ep.employee_id = :emp_id AND (ep.status = 1 OR ep.status = 'ACTIVE') AND ep.eligible = 1
            """)
            pos_df = pd.read_sql(positions_sql, conn, params={"emp_id": employee_id})
            header_data["eligible_positions"] = pos_df["description"].tolist()

            # KPIs
            
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

            # Cancelled Shifts (Employee Fault only: 2='< 24 Hours Notice', 3='> 24 Hours Notice')
            cancelled_sql = text("""
                SELECT COUNT(*) as cancelled_shifts
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                WHERE se.employee_id = :emp_id
                  AND se.cancel_reason IN (2, 3)
                  AND s.start <= NOW()
            """)
            cancelled_count = conn.execute(cancelled_sql, {"emp_id": employee_id}).scalar() or 0

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
                SELECT AVG(TIMESTAMPDIFF(MINUTE, s.start, t.employee_start)) as avg_variance,
                       COUNT(CASE WHEN TIMESTAMPDIFF(MINUTE, s.start, t.employee_start) <= 5 AND TIMESTAMPDIFF(MINUTE, s.start, t.employee_start) >= -5 THEN 1 END) as on_time_count,
                       COUNT(*) as total_punctual_shifts
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
            on_time_count = variance_row[1] if variance_row else 0
            total_punctual_shifts = variance_row[2] if variance_row else 0
            on_time_pct = round((on_time_count / total_punctual_shifts) * 100, 1) if total_punctual_shifts > 0 else None

            # Preferred Counts
            pref_sql = text("""
                SELECT COUNT(DISTINCT client_id) AS pref_clients, COUNT(DISTINCT venue_id) AS pref_venues
                FROM exclusive WHERE employee_id = :emp_id
            """)
            pref_row = conn.execute(pref_sql, {"emp_id": employee_id}).fetchone()
            pref_clients = pref_row[0] if pref_row else 0
            pref_venues = pref_row[1] if pref_row else 0

            # DNR Counts
            dnr_sql = text("""
                SELECT COUNT(DISTINCT client_id) AS dnr_clients, COUNT(DISTINCT venue_id) AS dnr_venues
                FROM dnr WHERE employee_id = :emp_id
            """)
            dnr_row = conn.execute(dnr_sql, {"emp_id": employee_id}).fetchone()
            dnr_clients = dnr_row[0] if dnr_row else 0
            dnr_venues = dnr_row[1] if dnr_row else 0

            # Avg Confirmation Time
            conf_time_sql = text("""
                SELECT AVG(TIMESTAMPDIFF(MINUTE, se.created_at, se.confirmed_at)) as avg_conf_time
                FROM shift_employee se
                JOIN user u ON se.request_by = u.id
                WHERE se.employee_id = :emp_id
                  AND u.`group` IN ('ADMIN', 'OWNER')
                  AND se.confirmed = 1
                  AND se.cancel_reason = 0
                  AND se.deleted_at IS NULL
                  AND se.confirmed_at IS NOT NULL
                  AND se.created_at IS NOT NULL
            """)
            conf_time_val = conn.execute(conf_time_sql, {"emp_id": employee_id}).scalar()
            avg_conf_time = round(float(conf_time_val), 1) if conf_time_val is not None else None

            # Optional Shift History
            history_sql = text("""
                SELECT 
                    e.date as event_date, 
                    c.name as client_name, 
                    v.name as venue_name, 
                    p.description as position_name, 
                    s.start as scheduled_start,
                    s.end as scheduled_end,
                    se.confirmed_at,
                    se.cancelled_at,
                    sr.reason as cancel_reason,
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
                LEFT JOIN status_reason sr ON se.cancel_reason = sr.id
                WHERE se.employee_id = :emp_id
                  AND e.date <= CURDATE()
                ORDER BY e.date DESC
                LIMIT 50
            """)
            history_df = pd.read_sql(history_sql, conn, params={"emp_id": employee_id})
            
            # fill na
            history_df = history_df.fillna("")
            # Convert datetime columns to strings
            for col in ['event_date', 'scheduled_start', 'scheduled_end', 'confirmed_at', 'cancelled_at', 'employee_start', 'employee_end']:
                if col in history_df.columns:
                    history_df[col] = history_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) and x != "" else "")

            shift_history = history_df.to_dict(orient="records")

            # Fetch DA Notes
            da_notes = []

            # 1. employee_note
            da_sql1 = text("""
                SELECT datetime AS created_at, type, note 
                FROM employee_note 
                WHERE employee_id = :emp_id 
                  AND type IN ('DAILY', 'PERSONNEL')
            """)
            da_res1 = conn.execute(da_sql1, {"emp_id": employee_id}).fetchall()
            pattern = re.compile(r'\b(flag|warning|ww|fww)\b', re.IGNORECASE)
            for row in da_res1:
                dt = row[0]
                n_type = row[1]
                n_text = row[2]
                if n_text and pattern.search(n_text):
                    da_notes.append({
                        "date": dt.isoformat() if pd.notnull(dt) else "",
                        "type": f"Note ({n_type})",
                        "text": n_text.strip(),
                        "timestamp": dt.timestamp() if pd.notnull(dt) else 0
                    })

            # 2. history_entry
            da_sql2 = text("""
                SELECT created_at, changes, notes 
                FROM history_entry 
                WHERE related = 'Employee' 
                  AND related_id = :emp_id 
                  AND model = 'Notification'
            """)
            da_res2 = conn.execute(da_sql2, {"emp_id": employee_id}).fetchall()
            warning_pattern = re.compile(r'\bwarning\b', re.IGNORECASE)
            for row in da_res2:
                dt = row[0]
                changes = row[1]
                n_notes = row[2]
                if changes and warning_pattern.search(changes):
                    text_content = changes
                    if n_notes:
                        text_content += f" | {n_notes}"
                    da_notes.append({
                        "date": dt.isoformat() if pd.notnull(dt) else "",
                        "type": "Notification",
                        "text": text_content.strip(),
                        "timestamp": dt.timestamp() if pd.notnull(dt) else 0
                    })

            # Sort da_notes by date descending
            da_notes.sort(key=lambda x: x["timestamp"], reverse=True)
            for n in da_notes:
                del n["timestamp"]

            return JSONResponse({
                "status": "success",
                "employee": header_data,
                "kpis": {
                    "worked_shifts": worked_count,
                    "cancelled_shifts": cancelled_count,
                    "noshow_shifts": noshow_count,
                    "avg_clock_in_variance_minutes": avg_variance,
                    "on_time_pct": on_time_pct,
                    "preferred_clients": pref_clients,
                    "preferred_venues": pref_venues,
                    "dnr_clients": dnr_clients,
                    "dnr_venues": dnr_venues,
                    "avg_confirmation_minutes": avg_conf_time
                },
                "shift_history": shift_history,
                "da_notes": da_notes
            })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

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
              AND (first_name LIKE :q OR last_name LIKE :q OR email LIKE :q OR mobile LIKE :q)
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
async def get_dashboard_reliability(employee_id: int):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse({"status": "error", "message": "OPENAI_API_KEY is not configured."}, status_code=500)
    
    engine = _engine()
    notes = []
    try:
        sql = text("""
            SELECT type, datetime, note
            FROM employee_note
            WHERE employee_id = :emp_id AND type IN ('PERSONNEL', 'DAILY')
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
                    
    except Exception as e:
        logger.exception("Failed to query employee notes")
        return JSONResponse({"status": "error", "message": "Database query failed"}, status_code=500)
    finally:
        engine.dispose()
        
    if not notes:
        return JSONResponse({
            "status": "success",
            "data": {
                "summary": "No personnel or daily notes on file.",
                "reliability_score": 100,
                "risk_factors": [],
                "positive_indicators": [],
                "notable_incidents": [],
                "caution": "No notes available to evaluate."
            }
        })
        
    notes_text = "\n".join(notes)
    
    prompt = f"""You are analyzing internal employee notes for a staffing manager.

Your task is to review the notes and produce a concise reliability assessment based only on the provided text.

Focus especially on:
- WW or written warning references
- lateness, tardiness, late arrivals, no-call/no-show, attendance issues
- behavior concerns such as rude, argumentative, disrespectful, unprofessional, conflict, attitude problems, complaints, harassment, aggression, refusal of assignment, or walking off a job
- repeated patterns across multiple notes
- positive indicators such as dependable, on time, professional, flexible, well-liked, praised by client, rehired, requested back, etc.

Scoring guidance:
- 90-100: highly reliable, no meaningful concerns, strong positive pattern
- 70-89: mostly reliable, minor concerns only
- 40-69: moderate concern, repeated lateness, warnings, or behavior issues
- 0-39: serious concern, repeated warnings, repeated attendance failures, or major behavior problems

Rules:
- Use only the notes provided.
- Do not invent details.
- Do not make legal, medical, or psychological conclusions.
- If notes are sparse, vague, or contradictory, state that in the caution field.
- Repeated and recent issues should weigh more heavily than isolated or older minor issues.
- Positive evidence should increase the score when clearly supported.

Return valid JSON only.
Use this exact schema:

{{
  "summary": "string",
  "reliability_score": 0,
  "risk_factors": ["string"],
  "positive_indicators": ["string"],
  "notable_incidents": ["string"],
  "caution": "string"
}}

Employee notes:
{notes_text}"""

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
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
                WHERE ep.employee_id = :emp_id AND ep.status = 'ACTIVE' AND ep.eligible = 1
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
                "shift_history": shift_history
            })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        engine.dispose()

import asyncio
import json
import logging
import os
import re
import random
from pathlib import Path

import httpx
import pypdf
import docx
import io
import openai
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Structured system prompt — mirrors Resume Analyzer qualification rules
# ---------------------------------------------------------------------------
POSITION_SYSTEM_PROMPT = """
You are a resume screener for a hospitality staffing agency.
The user will send you a resume (as text or transcribed from a PDF/Word doc/image)
together with self-reported experience text.
Your job is to decide how qualified the candidate is for specific hospitality positions.
Count experience at fine-dining or equivalent venues normally, but treat fast-food
experience differently (see updated rules below).

Target positions

Evaluate the candidate for these positions:

Cook
Prep Cook
Dishwasher
Utility
Server
Host
Runner
Busser
Bartender
Barback
Cashier
Pastry
Baker
Sushi
Concessions
Barista
Valet
Event Supervisor

Venue rules (VERY IMPORTANT)

Fast-food experience no longer disqualifies a candidate. Instead:

— Fast food or clearly quick-service chains (e.g., McDonald's, Burger King, Wendy's,
  Taco Bell, KFC, In-N-Out, Chick-fil-A, similar chains) DO qualify,
  BUT ONLY for Level 1 and ONLY if the role performed directly matches one of the
  target positions.

Examples:
• A McDonald's Cashier → qualifies for the Cashier position at Level 1.
• A Wendy's Cook → qualifies for the Cook position at Level 1.
• A Taco Bell Crew Member with cashier duties → qualifies for Cashier Level 1.
• A fast-food Shift Lead → does NOT qualify unless duties explicitly match one of the
  listed roles.

Fast-food experience should never count toward Level 2 or Level 3.

For Level 2 or Level 3 qualification:
Count only experience at fine dining or equivalent hospitality venues, such as:
— Hotels, resorts, country clubs
— Upscale restaurants, steakhouses, chef-driven or white-tablecloth concepts
— Banquet/catering companies, convention centers, stadiums, arenas, large event venues
— Corporate/contract dining for companies, universities, hospitals, etc., when clearly
  hospitality-related.

If a venue type is unclear and could reasonably be hospitality (e.g., "Italian
restaurant" without branding), you may count it with reduced confidence.

Ignore non-hospitality jobs entirely (admin, warehouse, rideshare, retail, etc.).

Special rule for Event Supervisor:
— This position requires a minimum of 3 years of management or supervisory experience in the hotel, food & beverage, or hospital industry.

Experience rules

For each position, you must:
1. Examine the entire work history and identify matching roles.
2. Estimate total time (in years) spent in those roles.

Experience categorization:
• Level 1: less than 2 years combined qualifying experience.
  — All fast-food experience ALWAYS counts as Level 1.
• Level 2: 2 to 5 years combined qualifying experience at non-fast-food venues.
• Level 3: more than 5 years qualifying experience at non-fast-food venues.

If the candidate has only fast-food experience for a role, assign Level 1
(never "no_experience").

If they have neither qualifying nor fast-food experience, assign "no_experience".

When estimating experience:
— Use job dates when available.
— Estimate approximate duration when missing.
— Avoid double counting overlapping jobs.

Output format

Return your result as valid JSON only, using this schema:
{
  "candidate_summary": {
    "hospitality_experience_overview": "",
    "total_hospitality_years_estimate": 0.0,
    "notable_venues": [],
    "notes_on_fast_food_or_non_qualifying_experience": ""
  },
  "positions": {
    "cook":        { "status": "no_experience | level_1 | level_2 | level_3", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "prep_cook":   { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "dishwasher":  { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "utility":     { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "server":      { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "host":        { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "runner":      { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "busser":      { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "bartender":   { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "barback":     { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "cashier":     { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "pastry":      { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "baker":       { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "sushi":       { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "concessions": { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "barista":     { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "valet":       { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] },
    "event_supervisor": { "status": "...", "estimated_years": 0.0, "confidence": 0.0, "reasons": [] }
  }
}

Confidence must be between 0.0 and 1.0.

In the reasons field, briefly explain:
— which roles and venues you counted,
— if fast-food experience was used to assign Level 1,
— why any other roles were excluded.

Do not include any text outside of the JSON.
"""

# Maps AI level strings → numeric tier
LEVEL_MAP = {
    "no_experience": 0,
    "level_1": 1,
    "level_2": 2,
    "level_3": 3,
}

# Maps position keys from AI JSON → base position name used in database
POSITION_KEY_TO_NAME = {
    "cook": "Cook",
    "prep_cook": "Prep Cook",
    "dishwasher": "Dishwasher",
    "utility": "Utility",
    "server": "Server",
    "host": "Host",
    "runner": "Runner",
    "busser": "Busser",
    "bartender": "Bartender",
    "barback": "Barback",
    "cashier": "Cashier",
    "pastry": "Pastry",
    "baker": "Baker",
    "sushi": "Sushi",
    "concessions": "Concessions",
    "barista": "Barista",
    "valet": "Valet",
    "event_supervisor": "Event Supervisor",
}
from sqlalchemy.engine import URL

DATA_FILE = Path("data/position_requests.json")
SEED_FILE = Path("apps/position_requests/seed_data.json")

def load_records():
    if SEED_FILE.exists():
        import shutil
        try:
            shutil.copy(SEED_FILE, DATA_FILE)
            SEED_FILE.unlink()
            print("Successfully seeded position_requests.json from seed_data.json")
        except Exception as e:
            print("Error seeding data:", e)

    if not DATA_FILE.exists():
        return []
    try:
        return json.loads(DATA_FILE.read_text())
    except:
        return []

def save_records(records):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(records, indent=2))

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

async def extract_text_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        jotform_key = os.getenv("JOTFORM_KEY", "")
        headers = {"APIKEY": jotform_key} if jotform_key else {}
        params = {"apiKey": jotform_key} if jotform_key else {}
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params, follow_redirects=True)
            if resp.status_code != 200:
                return f"Error downloading resume: Status {resp.status_code}"
            
            content = resp.content
            
            if content.strip().lower().startswith(b"<!doc") or content.strip().lower().startswith(b"<html"):
                return "JotForm Privacy Error: The file is blocked by a login screen. To fix this, go to your JotForm Account Settings -> Security, and UNCHECK the option 'Require log-in to view uploaded files'."
                
            url_lower = url.lower()
            if ".pdf" in url_lower:
                pdf_reader = pypdf.PdfReader(io.BytesIO(content))
                text_content = []
                for page in pdf_reader.pages:
                    if page.extract_text():
                        text_content.append(page.extract_text())
                return "\n".join(text_content)
            elif ".docx" in url_lower:
                doc = docx.Document(io.BytesIO(content))
                text_content = [para.text for para in doc.paragraphs]
                return "\n".join(text_content)
            else:
                try:
                    return content.decode("utf-8")
                except:
                    return "Unsupported file format for text extraction. Download it manually."
    except Exception as e:
        return f"Error extracting resume text: {str(e)}"

def _parse_requested_positions(raw: str) -> list[tuple[str, int]]:
    """Parse requested positions string into list of (base_name, level) tuples.

    Handles formats like:
      "Bartender 2, Cook 1"  →  [("Bartender", 2), ("Cook", 1)]
      "Bartender, Cook"      →  [("Bartender", 0), ("Cook", 0)]  (level 0 = any)
    """
    results = []
    # Split on comma, newline, or semicolons
    parts = re.split(r"[,;\n]+", raw)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Try to extract trailing level number: "Bartender 2" → ("Bartender", 2)
        m = re.match(r"^(.+?)\s+(\d)$", part)
        if m:
            results.append((m.group(1).strip(), int(m.group(2))))
        else:
            results.append((part.strip(), 0))
    return results


def _match_position_key(name: str) -> str | None:
    """Fuzzy-match a requested position name to an AI JSON key."""
    name_lower = name.lower().strip()
    # Direct key match
    if name_lower.replace(" ", "_") in POSITION_KEY_TO_NAME:
        return name_lower.replace(" ", "_")
    # Match against base names
    for key, base in POSITION_KEY_TO_NAME.items():
        if name_lower == base.lower():
            return key
        # Partial / starts-with
        if base.lower().startswith(name_lower) or name_lower.startswith(base.lower()):
            return key
    return None


def _evaluate_positions_against_levels(
    ai_positions: dict, requested_positions: str
) -> tuple[str, str, list[str]]:
    """Compare AI-determined levels against requested position levels.

    Returns (status, explanation_text, approved_position_names).
    """
    parsed = _parse_requested_positions(requested_positions)
    if not parsed:
        return "Consider", "Could not parse any requested positions.", []

    approved = []
    denied = []
    details = []

    for req_name, req_level in parsed:
        key = _match_position_key(req_name)
        if not key:
            details.append(f"• {req_name}: could not match to a known position.")
            denied.append(req_name)
            continue

        pos_data = ai_positions.get(key, {})
        ai_status = pos_data.get("status", "no_experience")
        ai_level = LEVEL_MAP.get(ai_status, 0)
        est_years = pos_data.get("estimated_years", 0)
        reasons = pos_data.get("reasons", [])
        base_name = POSITION_KEY_TO_NAME.get(key, req_name)

        # If no level was specified in the request, approve at whatever level they qualify for
        if req_level == 0:
            if ai_level >= 1:
                label = f"{base_name} {ai_level}"
                approved.append(label)
                details.append(
                    f"✅ {req_name} → Qualified at Level {ai_level} "
                    f"({est_years:.1f} yrs). {'; '.join(reasons)}"
                )
            else:
                denied.append(req_name)
                details.append(
                    f"❌ {req_name} → No qualifying experience found. "
                    f"{'; '.join(reasons)}"
                )
        else:
            # Specific level requested — candidate must meet or exceed it
            if ai_level >= req_level:
                label = f"{base_name} {req_level}"
                approved.append(label)
                details.append(
                    f"✅ {req_name} {req_level} → Qualified (AI Level {ai_level}, "
                    f"{est_years:.1f} yrs). {'; '.join(reasons)}"
                )
            else:
                denied.append(f"{req_name} {req_level}")
                qualified_label = f"Level {ai_level}" if ai_level > 0 else "no experience"
                details.append(
                    f"❌ {req_name} {req_level} → NOT qualified. "
                    f"Only qualifies at {qualified_label} "
                    f"({est_years:.1f} yrs). {'; '.join(reasons)}"
                )

    detail_text = "\n".join(details)

    if denied and not approved:
        status = "Not Approved"
    elif denied and approved:
        status = "Consider"
        detail_text = (
            f"Approved: {', '.join(approved)}\n"
            f"Denied: {', '.join(denied)}\n\n"
            + detail_text
        )
    else:
        status = "Approved"

    return status, detail_text, approved


async def ai_analyze(resume_text, experience_text, requested_positions):
    """Run structured AI analysis and return (status, analysis_text, approved_positions)."""
    combined_text = f"User provided experience:\n{experience_text}\n\nResume text:\n{resume_text}"
    user_msg = (
        f"Resume and experience information for evaluation. "
        f"Return only the JSON schema provided.\n\n{combined_text}"
    )
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": POSITION_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or ""
        parsed = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("AI response was not valid JSON — falling back to Consider")
        return "Consider", f"AI returned non-JSON response:\n{content}", []
    except Exception as e:
        return "Consider", f"AI Analysis Error: {str(e)}", []

    ai_positions = parsed.get("positions", {})
    summary = parsed.get("candidate_summary", {})

    status, detail_text, approved = _evaluate_positions_against_levels(
        ai_positions, requested_positions
    )

    # Prepend candidate overview
    overview = summary.get("hospitality_experience_overview", "")
    total_yrs = summary.get("total_hospitality_years_estimate", 0)
    venues = summary.get("notable_venues", [])
    ff_notes = summary.get("notes_on_fast_food_or_non_qualifying_experience", "")

    header = (
        f"Candidate Overview:\n"
        f"  Total hospitality experience: ~{total_yrs:.1f} years\n"
    )
    if venues:
        header += f"  Notable venues: {', '.join(venues)}\n"
    if overview:
        header += f"  {overview}\n"
    if ff_notes:
        header += f"  Fast-food notes: {ff_notes}\n"
    header += "\nPosition Evaluation:\n"

    full_analysis = header + detail_text
    return status, full_analysis, approved

async def evaluate_candidate(phone, resume_url, experience_text, requested_positions):
    engine = _engine()
    status = "Consider"
    ai_analysis = ""
    
    clean_phone = re.sub(r'\D', '', phone)
    if len(clean_phone) > 10:
        clean_phone = clean_phone[-10:]
        
    if not clean_phone or len(clean_phone) < 10:
        engine.dispose()
        return "Consider", "No valid 10-digit phone number provided for matching. Marked as Consider."

    try:
        with engine.connect() as conn:
            emp_sql = text("""
                SELECT employee_id 
                FROM employee 
                WHERE REPLACE(REPLACE(REPLACE(REPLACE(mobile, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                   OR REPLACE(REPLACE(REPLACE(REPLACE(home, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                   OR REPLACE(REPLACE(REPLACE(REPLACE(work, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                LIMIT 1
            """)
            emp_res = conn.execute(emp_sql, {"phone": f"%{clean_phone}%"}).fetchone()
            
            if not emp_res:
                return "Consider", "Employee not found in database. Marked as Consider."
            
            emp_id = emp_res[0]
            
            dnr_sql = text("""
                SELECT employee_id FROM dnr WHERE employee_id = :emp_id AND created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR) LIMIT 1
            """)
            has_dnr = conn.execute(dnr_sql, {"emp_id": emp_id}).fetchone() is not None
            
            da_sql = text("""
                SELECT id FROM history_entry WHERE related = 'Employee' AND related_id = :emp_id AND created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR) AND changes LIKE '%Warning%' LIMIT 1
            """)
            has_da = conn.execute(da_sql, {"emp_id": emp_id}).fetchone() is not None
            
            noshow_sql = text("""
                SELECT t.timesheet_id FROM timesheet t 
                JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id 
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id 
                JOIN shift s ON sp.shift_id = s.shift_id 
                WHERE t.employee_id = :emp_id 
                  AND t.employee_worked = 'NOSHOW' 
                  AND s.start >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
                  AND s.start <= DATE_SUB(NOW(), INTERVAL 10 DAY)
                LIMIT 1
            """)
            has_noshow = conn.execute(noshow_sql, {"emp_id": emp_id}).fetchone() is not None
            
            if has_dnr or has_da or has_noshow:
                reasons = []
                if has_dnr: reasons.append("DNR in last 2 years")
                if has_da: reasons.append("DA (Warning) in last 2 years")
                if has_noshow: reasons.append("NOSHOW between 10 days and 2 years ago")
                return "Not Approved", "Automatically marked as Not Approved due to: " + ", ".join(reasons)
                
    except Exception as e:
        engine.dispose()
        return "Consider", f"Database error during checking: {str(e)}"
    finally:
        engine.dispose()
        
    resume_text = await extract_text_from_url(resume_url) if resume_url else "No resume attached."
    status, analysis_text, _ = await ai_analyze(resume_text, experience_text, requested_positions)
    return status, analysis_text

async def fetch_submissions():
    jotform_key = os.getenv("JOTFORM_KEY")
    if not jotform_key:
        print("Missing JOTFORM_KEY for position requests")
        return
        
    form_id = "240575384332053"
    url = f"https://api.jotform.com/form/{form_id}/submissions"
    
    headers = {
        "APIKEY": jotform_key
    }
    
    # We can fetch latest 100 submissions
    params = {
        "limit": 100
    }

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            if data.get("responseCode") != 200:
                print("Jotform error:", data.get("message"))
                return
            submissions = data.get("content", [])
        except Exception as e:
            print("Error fetching submissions for position requests:", str(e))
            return

        records = load_records()
        
        reprocess_flag = Path("data/reprocess_event_supervisor.flag")
        _is_render = any(os.getenv(v) for v in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL"))
        
        if _is_render and not reprocess_flag.exists():
            reprocess_flag.parent.mkdir(parents=True, exist_ok=True)
            reprocess_flag.write_text("done")
            for r in records:
                if "Event Supervisor" in r.get("positions", ""):
                    r["status"] = "Pending"
            save_records(records)

        existing_ids = {r.get("message_id") for r in records if r.get("message_id")}
        added = False

        # Process from oldest to newest conceptually if we reverse Jotform's default descending order
        # But actually Jotform sorted newest first. Reversing helps apply oldest to records.insert(0, ...)
        for sub in reversed(submissions):
            sub_id = str(sub.get("id"))
            if sub_id in existing_ids:
                continue
                
            answers = sub.get("answers", {})
            name = "Unknown"
            phone = ""
            positions = ""
            experience = ""
            resume_link = ""

            for key, val in answers.items():
                text_label = val.get("text", "").lower()
                answer_val = val.get("answer", "")
                
                if not answer_val:
                    continue

                if "name" in text_label:
                    if isinstance(answer_val, dict):
                        name = f"{answer_val.get('first', '')} {answer_val.get('last', '')}".strip()
                    else:
                        name = str(answer_val)
                elif "phone" in text_label:
                    if isinstance(answer_val, dict):
                        phone = str(answer_val.get('full', answer_val))
                    else:
                        phone = str(answer_val)
                elif "positions" in text_label:
                    # Depending on Jotform widget, it could be a list or comma string
                    if isinstance(answer_val, list):
                        positions = ", ".join(map(str, answer_val))
                    elif isinstance(answer_val, dict):
                        positions = ", ".join(f"{k}: {v}" for k, v in answer_val.items() if v)
                    else:
                        positions = str(answer_val)
                elif "resume" in text_label:
                    if isinstance(answer_val, list):
                        resume_link = str(answer_val[0]) if answer_val else ""
                    else:
                        resume_link = str(answer_val)
                elif "experience" in text_label:
                    # In case they put a URL directly into the experience text box
                    val_str = str(answer_val).strip()
                    if val_str.startswith("http") and not "\n" in val_str and not " " in val_str:
                        if not resume_link:
                            resume_link = val_str
                    else:
                        experience = val_str

            recruiter = random.choices(["Piyush", "Mercedes"], weights=[60, 40], k=1)[0]
            
            status, ai_analysis = await evaluate_candidate(phone, resume_link, experience, positions)

            new_record = {
                "message_id": sub_id,  # Using submission id as message_id for compatibility
                "employee_name": name,
                "phone": phone,
                "positions": positions[:500],
                "experience": experience,
                "resume_link": resume_link,
                "recruiter": recruiter,
                "completed": False,
                "status": status,
                "ai_analysis": ai_analysis,
                "received_date": sub.get("created_at", "")
            }
            records.insert(0, new_record)
            added = True
            
        # Backfill any existing records that somehow lack a status (e.g., loaded from persistent disk before this feature)
        for record in records:
            if record.get("status") in [None, "", "Pending"] or "Database error during checking" in record.get("ai_analysis", ""):
                try:
                    status, ai_analysis = await evaluate_candidate(
                        record.get("phone", ""), 
                        record.get("resume_link", ""), 
                        record.get("experience", ""), 
                        record.get("positions", "")
                    )
                    record["status"] = status
                    record["ai_analysis"] = ai_analysis
                    added = True
                    save_records(records)
                    await asyncio.sleep(2)  # Process one at a time and yield to event loop
                except Exception as e:
                    print("Error during backfill in fetch_submissions:", str(e))
            
        if added:
            save_records(records)

async def position_requests_monitoring_loop():
    while True:
        try:
            await fetch_submissions()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("Error in position_requests_monitoring_loop:", str(e))
        
        await asyncio.sleep(60 * 15)  # 15 minutes

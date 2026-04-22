import asyncio
import json
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
from sqlalchemy.engine import URL

DATA_FILE = Path("data/position_requests.json")

def load_records():
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

async def ai_analyze(resume_text, experience_text, requested_positions):
    combined_text = f"User provided experience:\n{experience_text}\n\nResume text:\n{resume_text}"
    prompt = f"""
The candidate is applying for the following requested positions: {requested_positions}

Here is the candidate's experience information:
{combined_text}

Analyze the candidate's experience and determine if they are qualified for the requested positions. Give a clear, concise summary of your findings.
Start your response with either "APPROVED" or "NOT APPROVED" on the very first line, followed by your explanation.
"""
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"AI Analysis Error: {str(e)}"

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
    analysis_text = await ai_analyze(resume_text, experience_text, requested_positions)
    
    if analysis_text.strip().upper().startswith("APPROVED"):
        status = "Approved"
    elif analysis_text.strip().upper().startswith("NOT APPROVED"):
        status = "Not Approved"
    else:
        status = "Consider"
        
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

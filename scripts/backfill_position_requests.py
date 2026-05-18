import asyncio
import sys
import os
import io
import json
import base64
from urllib.parse import quote
from dotenv import load_dotenv
from sqlalchemy import text
import httpx
import pypdf
import docx
import openai

# Load env vars
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

# Add root to python path
sys.path.insert(0, os.path.abspath('.'))

from apps.position_requests.scheduler import _engine, POSITION_SYSTEM_PROMPT, LEVEL_MAP

S3_BUCKET = "web-application-files"
S3_PREFIX = "employee/resume/"
S3_REGION = "us-east-1"

OPENAI_API_KEY = os.getenv("STAND_ALONE") or os.getenv("OPENAI_API_KEY")

async def extract_text_from_image(content: bytes, mime_type: str) -> str:
    """Extract text from an image using OpenAI Vision model."""
    safe_mime = mime_type if mime_type.startswith("image/") else "image/png"
    encoded = base64.b64encode(content).decode("utf-8")
    data_url = f"data:{safe_mime};base64,{encoded}"
    
    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Transcribe all the text in this resume image exactly as it appears. Return only the transcribed text, without any additional commentary."
                        },
                        {"type": "image_url", "image_url": {"url": data_url, "detail": "low"}}
                    ]
                }
            ],
            temperature=0.0
        )
        return response.choices[0].message.content or ""
    except Exception as e:
        return f"Error extracting text from image: {str(e)}"

async def extract_resume_text(filename: str) -> str:
    """Download the resume from S3 or URL and extract text."""
    if filename.startswith("http"):
        url = filename
    else:
        safe_filename = quote(filename)
        url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{S3_PREFIX}{safe_filename}"
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, follow_redirects=True)
            
            if resp.status_code != 200:
                return f"Error: Could not download resume (Status {resp.status_code})."
            
            content = resp.content
            filename_lower = filename.lower()
            
            if ".pdf" in filename_lower:
                pdf_reader = pypdf.PdfReader(io.BytesIO(content))
                text_content = []
                for page in pdf_reader.pages:
                    extracted = page.extract_text()
                    if extracted:
                        text_content.append(extracted)
                return "\n".join(text_content)
                
            elif ".docx" in filename_lower:
                doc = docx.Document(io.BytesIO(content))
                text_content = [para.text for para in doc.paragraphs]
                return "\n".join(text_content)
                
            elif filename_lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
                mime_type = "image/jpeg"
                if filename_lower.endswith(".png"):
                    mime_type = "image/png"
                elif filename_lower.endswith(".webp"):
                    mime_type = "image/webp"
                
                print("  -> Image detected, transcribing text via AI...")
                return await extract_text_from_image(content, mime_type)
            else:
                try:
                    return content.decode("utf-8")
                except Exception:
                    return "Error: Unsupported file format for text extraction."
                    
    except Exception as e:
        return f"Error downloading/extracting resume: {str(e)}"

async def ai_analyze_standalone(resume_text: str):
    """Run structured AI analysis."""
    user_msg = (
        f"Resume information for evaluation.\n"
        f"Return only the JSON schema provided.\n\n{resume_text}"
    )
    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": POSITION_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or ""
        return json.loads(content)
    except Exception as e:
        print(f"AI Analysis Error: {str(e)}")
        return {}

async def main():
    try:
        from apps.api_usage_tracker import log_api_usage
        log_api_usage("Stand Alone Scripts")
    except ImportError:
        pass

    engine = _engine()
    
    # Set this to the last processed employee_id if you need to resume
    START_ID = 0  # Starting from 0 to run the whole list
    
    try:
        # 1. Fetch the list of employees and close connection immediately
        try:
            with engine.connect() as conn:
                emp_sql = text("""
                    SELECT 
                        employee_id, 
                        COALESCE(mobile, home, work) as phone, 
                        resume, 
                        CONCAT(first_name, ' ', last_name) as name 
                    FROM employee 
                    WHERE status = 10 
                      AND resume IS NOT NULL 
                      AND resume != ''
                      AND employee_id > :start_id
                    ORDER BY employee_id ASC
                """)
                employees = conn.execute(emp_sql, {"start_id": START_ID}).fetchall()
        except Exception as e:
            print(f"Error fetching employee list: {e}")
            return

        print(f"Found {len(employees)} inactive (60) employees with resumes.")
        
        id_to_name = {
            115: "Server 2",
            110: "Prep Cook 2",
            102: "Cook 2",
            264: "Event Supervisor"
        }
        
        # 2. Process each employee
        for emp in employees:
            emp_id = emp.employee_id
            phone = emp.phone or ''
            resume_file = emp.resume
            name = emp.name
            
            # Check their existing eligible positions using a fresh connection
            try:
                with engine.connect() as conn:
                    pos_sql = text("""
                        SELECT position_id
                        FROM employee_position
                        WHERE employee_id = :emp_id
                          AND position_id IN (115, 110, 102, 264)
                          AND status = 1
                          AND eligible = 1
                    """)
                    existing_positions = conn.execute(pos_sql, {"emp_id": emp_id}).fetchall()
            except Exception as e:
                print(f"Error checking positions for {name} (ID: {emp_id}): {e}")
                continue

            already_eligible_ids = {row.position_id for row in existing_positions}
            needed_ids = [pid for pid in id_to_name.keys() if pid not in already_eligible_ids]
            needed = [id_to_name[pid] for pid in needed_ids]
            
            if not needed:
                print(f"Skipping {name} (ID: {emp_id}): Already eligible for all target positions.")
                continue
                
            requested_positions = ", ".join(needed)
            print(f"Evaluating {name} (ID: {emp_id}) for: {requested_positions}")
            
            # 1. Download and extract text
            text_content = await extract_resume_text(resume_file)
            if text_content.startswith("Error"):
                print(f"  -> {text_content}")
                continue
                
            # 2. Evaluate with AI
            ai_positions = await ai_analyze_standalone(text_content)
            if not ai_positions:
                continue
                
            positions_eval = ai_positions.get("positions", {})
            qualified_for = []
            
            # Event Supervisor check (requires Level 2+ and >= 3.0 years, or Level 3)
            if 264 in needed_ids:
                es_eval = positions_eval.get("event_supervisor", {})
                es_level = LEVEL_MAP.get(es_eval.get("status", "no_experience"), 0)
                es_years = es_eval.get("estimated_years", 0)
                if (es_level >= 2 and es_years >= 3.0) or es_level >= 3:
                    qualified_for.append(("Event Supervisor", 264))
                    
            # Server 2 check (requires Level 2+)
            if 115 in needed_ids:
                s_eval = positions_eval.get("server", {})
                s_level = LEVEL_MAP.get(s_eval.get("status", "no_experience"), 0)
                if s_level >= 2:
                    qualified_for.append(("Server 2", 115))
                    
            # Prep Cook 2 check (requires Level 2+)
            if 110 in needed_ids:
                pc_eval = positions_eval.get("prep_cook", {})
                pc_level = LEVEL_MAP.get(pc_eval.get("status", "no_experience"), 0)
                if pc_level >= 2:
                    qualified_for.append(("Prep Cook 2", 110))
                    
            # Cook 2 check (requires Level 2+)
            if 102 in needed_ids:
                c_eval = positions_eval.get("cook", {})
                c_level = LEVEL_MAP.get(c_eval.get("status", "no_experience"), 0)
                if c_level >= 2:
                    qualified_for.append(("Cook 2", 102))

            if qualified_for:
                names = [q[0] for q in qualified_for]
                print(f"  -> 🎉 QUALIFIED FOR: {', '.join(names)}")
                
                # Update the database
                try:
                    with engine.begin() as conn:
                        for name, pos_id in qualified_for:
                            existing = conn.execute(
                                text("""
                                    SELECT employee_position_id
                                    FROM employee_position
                                    WHERE employee_id = :employee_id AND position_id = :position_id
                                    LIMIT 1
                                """),
                                {"employee_id": emp_id, "position_id": pos_id},
                            ).fetchone()
            
                            if existing:
                                conn.execute(
                                    text("""
                                        UPDATE employee_position
                                        SET eligible = 1, status = 1
                                        WHERE employee_position_id = :employee_position_id
                                    """),
                                    {"employee_position_id": existing[0]},
                                )
                            else:
                                conn.execute(
                                    text("""
                                        INSERT INTO employee_position (employee_id, position_id, eligible, status, sub_type_id)
                                        VALUES (:employee_id, :position_id, 1, 1, -1)
                                    """),
                                    {"employee_id": emp_id, "position_id": pos_id},
                                )
                        print("  -> ✅ Added to database!")
                except Exception as e:
                    print(f"  -> ❌ Error updating database: {e}")
            else:
                print("  -> Not qualified for any needed positions.")
                
    finally:
        engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())

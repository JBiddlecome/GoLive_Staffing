import os
import json
import asyncio
import io
from urllib.parse import quote

import httpx
import pypdf
import docx
import pandas as pd
from sqlalchemy import create_engine, text
import openai
from dotenv import load_dotenv

# Load environment variables
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
# Set your OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# S3 Configuration
S3_BUCKET = "web-application-files"
S3_PREFIX = "employee/resume/"
S3_REGION = "us-east-1"

# Database Configuration fallback logic
def _get_db_url():
    host = os.getenv("REPORTABLE_DB_HOST") or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port_str = os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306")
    port = int(port_str)

    if host in {"127.0.0.1", "localhost"} and not os.getenv("REPORTABLE_DB_HOST"):
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    from sqlalchemy.engine import URL
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

# ---------------------------------------------------------
# AI Prompt
# ---------------------------------------------------------
EVALUATION_PROMPT = """
You are an expert hospitality recruiter.
You will receive the text extracted from a candidate's resume.
Your task is to determine if this candidate qualifies for the specified Level 2 positions: {roles_to_evaluate}.

Rules for Qualification (based on Level 2 requirements):
1. Fast-food or quick-service chain experience (e.g., McDonald's, Taco Bell) does NOT count toward qualification.
2. Qualifying experience must be at fine dining, hotels, upscale restaurants, catering companies, or equivalent venues.
3. The candidate must have at least 2 years of qualifying experience in a matching role to be approved for a Level 2 position.

Position Specifics:
- "Cook 2" (Line Cook Level 2): Requires 2+ years of intermediate line cook experience at qualifying venues.
- "Prep Cook 2" (Prep Cook Level 2): Requires 2+ years of prep cook experience at qualifying venues.
- "Server 2" (Server Level 2): Requires 2+ years of serving experience at qualifying venues.

Return your response as a valid JSON object with the following schema:
{{
  "qualifies_cook_2": true | false | null,
  "qualifies_prep_cook_2": true | false | null,
  "qualifies_server_2": true | false | null,
  "years_of_experience": 0.0,
  "reasoning": "Brief explanation detailing the qualifying venues, fast-food exclusions, and how years of experience were calculated."
}}
Note: Set 'qualifies_...' to null if you were NOT asked to evaluate that specific role.
"""

async def extract_resume_text(filename: str) -> str:
    """Download the resume from S3 and extract text."""
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
                
            else:
                try:
                    return content.decode("utf-8")
                except Exception:
                    return "Error: Unsupported file format for text extraction."
                    
    except Exception as e:
        return f"Error downloading/extracting resume: {str(e)}"


async def evaluate_candidate(resume_text: str, roles_to_evaluate: list) -> dict:
    """Pass the resume text to OpenAI to check for experience."""
    if "Error" in resume_text:
        return {"qualifies_cook_2": False, "qualifies_prep_cook_2": False, "qualifies_server_2": False, "reasoning": resume_text, "years_of_experience": 0}
        
    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        prompt = EVALUATION_PROMPT.format(roles_to_evaluate=", ".join(roles_to_evaluate))
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Resume Text:\n\n{resume_text[:15000]}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)
        
    except Exception as e:
        return {"qualifies_cook_2": False, "qualifies_prep_cook_2": False, "qualifies_server_2": False, "reasoning": f"AI Error: {str(e)}", "years_of_experience": 0}


async def main():
    print("Connecting to database...")
    engine = create_engine(_get_db_url())
    
    # Query all active employees with a resume, including their positions
    query = text("""
        SELECT e.employee_id, e.first_name, e.last_name, e.email, e.mobile, e.resume,
               GROUP_CONCAT(ep.position_id) as positions
        FROM employee e
        LEFT JOIN employee_position ep ON e.employee_id = ep.employee_id AND ep.status = 1
        WHERE e.status = 1 
          AND e.resume IS NOT NULL 
          AND e.resume != ''
          AND e.deleted_at IS NULL
        GROUP BY e.employee_id
    """)
    
    with engine.connect() as conn:
        active_employees = conn.execute(query).fetchall()
        
    print(f"Found {len(active_employees)} active employees with resumes.")
    
    results = []
    
    for i, emp in enumerate(active_employees):
        emp_id, first, last, email, mobile, resume_file, positions = emp
        print(f"[{i+1}/{len(active_employees)}] Checking {first} {last}...")
        
        positions_str = str(positions) if positions else ""
        positions_list = positions_str.split(",")
        has_cook2 = "102" in positions_list
        has_prep2 = "110" in positions_list
        has_server2 = "115" in positions_list

        check_cook2 = not has_cook2
        check_prep2 = not has_prep2
        check_server2 = not has_server2

        if not check_cook2 and not check_prep2 and not check_server2:
            print("  --> Skipping. Already has Cook 2, Prep Cook 2, and Server 2.")
            continue

        roles_to_evaluate = []
        if check_cook2:
            roles_to_evaluate.append("Cook 2")
        if check_prep2:
            roles_to_evaluate.append("Prep Cook 2")
        if check_server2:
            roles_to_evaluate.append("Server 2")
            
        print(f"  --> Need to evaluate for: {', '.join(roles_to_evaluate)}")

        # 1. Download and extract text
        text_content = await extract_resume_text(resume_file)
        
        # 2. Evaluate with AI
        evaluation = await evaluate_candidate(text_content, roles_to_evaluate)
        
        # 3. Process results
        qual_cook2 = evaluation.get("qualifies_cook_2")
        qual_prep2 = evaluation.get("qualifies_prep_cook_2")
        qual_server2 = evaluation.get("qualifies_server_2")
        
        is_qualified = (qual_cook2 is True) or (qual_prep2 is True) or (qual_server2 is True)
        
        if is_qualified:
            qualified_for = []
            if qual_cook2 is True:
                qualified_for.append("Cook 2")
            if qual_prep2 is True:
                qualified_for.append("Prep Cook 2")
            if qual_server2 is True:
                qualified_for.append("Server 2")
                
            print(f"  --> 🎉 QUALIFIED: {', '.join(qualified_for)}")
            results.append({
                "Employee ID": emp_id,
                "Name": f"{first} {last}",
                "Email": email,
                "Phone": mobile,
                "Currently Has Cook 2": "Yes" if has_cook2 else "No",
                "Currently Has Prep Cook 2": "Yes" if has_prep2 else "No",
                "Currently Has Server 2": "Yes" if has_server2 else "No",
                "Newly Qualified For": ", ".join(qualified_for),
                "Years Experience": evaluation.get("years_of_experience", 0),
                "AI Reasoning": evaluation.get("reasoning", ""),
                "Resume Link": f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{S3_PREFIX}{quote(resume_file)}"
            })
        else:
            print("  --> Not qualified.")
            
    # Save results to a CSV/Excel file
    if results:
        df = pd.DataFrame(results)
        output_file = "candidates_report.xlsx"
        df.to_excel(output_file, index=False)
        print(f"\n✅ Analysis complete! Found {len(results)} new qualified candidates.")
        print(f"Results saved to {output_file}")
    else:
        print("\nAnalysis complete. No new qualified candidates found.")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception as e:
        print("\nAn error occurred:")
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")

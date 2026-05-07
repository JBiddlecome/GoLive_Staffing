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

# Load environment variables from .env file
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

    # Escape password if needed (pymysql handles basic auth in URL, but safer to use sqlalchemy.engine.URL if complex)
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
SUPERVISOR_PROMPT = """
You are an expert hospitality recruiter.
You will receive the text extracted from a candidate's resume.
Your sole job is to determine if this candidate has explicit experience as an "Event Supervisor", "Banquet Captain", "Catering Manager", "Front of House Manager", or equivalent leadership/supervisory role in a hospitality or events setting.

Rules:
1. Being a standard "Server", "Bartender", or "Cook" does NOT qualify. They must have held a role where they were actively managing, supervising, or leading an event team.
2. Fast food shift-lead experience does NOT qualify. It must be in event, banquet, or restaurant leadership.
3. Look for keywords like "Supervisor", "Captain", "Manager", "Lead", "Director".

Return your response as a valid JSON object with the following schema:
{
  "qualifies": true | false,
  "relevant_titles": ["Captain", "Event Supervisor", ...],
  "years_of_experience": 0.0,
  "reasoning": "Brief explanation of why they do or do not qualify based on their experience."
}
"""

async def extract_resume_text(filename: str) -> str:
    """Download the resume from S3 and extract text."""
    # Since the S3 bucket is configured for 'public-read' in your Yii2 app,
    # we can try accessing it directly via HTTP. If it's private, you'll need boto3.
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


async def evaluate_supervisor(resume_text: str) -> dict:
    """Pass the resume text to OpenAI to check for supervisor experience."""
    if "Error" in resume_text:
        return {"qualifies": False, "reasoning": resume_text, "relevant_titles": [], "years_of_experience": 0}
        
    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4.1-mini",  # Using your preferred model
            messages=[
                {"role": "system", "content": SUPERVISOR_PROMPT},
                {"role": "user", "content": f"Resume Text:\n\n{resume_text[:15000]}"} # Limit tokens
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)
        
    except Exception as e:
        return {"qualifies": False, "reasoning": f"AI Error: {str(e)}", "relevant_titles": [], "years_of_experience": 0}


async def main():
    print("Connecting to database...")
    engine = create_engine(_get_db_url())
    
    # Query all active employees with a resume
    query = text("""
        SELECT employee_id, first_name, last_name, email, mobile, resume
        FROM employee
        WHERE status = 1 
          AND resume IS NOT NULL 
          AND resume != ''
          AND deleted_at IS NULL
    """)
    
    with engine.connect() as conn:
        active_employees = conn.execute(query).fetchall()
        
    print(f"Found {len(active_employees)} active employees with resumes.")
    
    results = []
    
    # Process sequentially to avoid rate-limiting OpenAI or network spikes
    # You can change this to asyncio.gather if you want to run batches in parallel
    for i, emp in enumerate(active_employees):
        emp_id, first, last, email, mobile, resume_file = emp
        print(f"[{i+1}/{len(active_employees)}] Analyzing {first} {last}...")
        
        # 1. Download and extract text
        text_content = await extract_resume_text(resume_file)
        
        # 2. Evaluate with AI
        evaluation = await evaluate_supervisor(text_content)
        
        # 3. Only keep those who qualify to keep the final list clean
        if evaluation.get("qualifies") is True:
            print(f"  --> 🎉 QUALIFIED: {', '.join(evaluation.get('relevant_titles', []))}")
            results.append({
                "Employee ID": emp_id,
                "Name": f"{first} {last}",
                "Email": email,
                "Phone": mobile,
                "Relevant Titles": ", ".join(evaluation.get("relevant_titles", [])),
                "Years Experience": evaluation.get("years_of_experience", 0),
                "AI Reasoning": evaluation.get("reasoning", ""),
                "Resume Link": f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{S3_PREFIX}{quote(resume_file)}"
            })
        else:
            print("  --> Not qualified.")
            
    # Save results to a CSV/Excel file
    if results:
        df = pd.DataFrame(results)
        output_file = "event_supervisors_report.xlsx"
        df.to_excel(output_file, index=False)
        print(f"\n✅ Analysis complete! Found {len(results)} qualified candidates.")
        print(f"Results saved to {output_file}")
    else:
        print("\nAnalysis complete. No qualified Event Supervisors found.")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception as e:
        print("\\nAn error occurred:")
        traceback.print_exc()
    finally:
        input("\\nPress Enter to exit...")

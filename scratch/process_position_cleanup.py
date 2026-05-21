import os
import sys
import asyncio
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from urllib.parse import quote

# Load env variables
load_dotenv(r"c:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

# Override API key
os.environ["OPENAI_API_KEY"] = os.environ.get("STAND_ALONE", "")
os.environ["POSTION_REQUESTS"] = os.environ.get("STAND_ALONE", "")
os.environ["RESUME_ANALYZER"] = os.environ.get("STAND_ALONE", "")

# Add project root to sys.path so apps module can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from apps.position_requests.scheduler import _engine, extract_text_from_url, ai_analyze

async def process_row(row, engine):
    payroll_id = row.get('Payroll ID')
    positions_req = row.get('Positions', '')
    experience_notes = row.get('Experience Notes', '')
    
    if pd.isna(payroll_id):
        return row
        
    payroll_id_str = str(payroll_id).strip()
    # Try to convert float like 12345.0 to 12345
    if payroll_id_str.endswith(".0"):
        payroll_id_str = payroll_id_str[:-2]
    
    # Get resume from DB
    resume_url = ""
    try:
        with engine.connect() as conn:
            sql = text("SELECT resume FROM employee WHERE payroll_id = :payroll_id LIMIT 1")
            res = conn.execute(sql, {"payroll_id": payroll_id_str}).fetchone()
            if res and res[0]:
                resume_url = res[0]
                # If resume_url is just a filename, construct the S3 URL
                if not resume_url.startswith("http"):
                    s3_bucket = os.getenv("S3_BUCKET", "web-application-files")
                    s3_region = os.getenv("S3_REGION", "us-east-1")
                    s3_prefix = os.getenv("RESUME_S3_PREFIX", "employee/resume/")
                    safe_filename = quote(resume_url)
                    resume_url = f"https://{s3_bucket}.s3.{s3_region}.amazonaws.com/{s3_prefix}{safe_filename}"
    except Exception as e:
        print(f"Error fetching resume for {payroll_id_str}: {e}")
        
    resume_text = "No resume attached."
    if resume_url:
        resume_text = await extract_text_from_url(resume_url)
        if "Error" in resume_text or "Unsupported" in resume_text:
             print(f"Warning extracting resume for {payroll_id_str}: {resume_text}")
             resume_text = "No resume attached."
        
    # Analyze
    status, ai_analysis, approved_positions = await ai_analyze(resume_text, experience_notes, str(positions_req))
    
    row['Approved Positions'] = ", ".join(approved_positions)
    row['Status Output'] = status
    row['AI Analysis Details'] = ai_analysis
    
    return row

async def main():
    input_file = 'scratch/position_cleanup_copy.xlsx'
    output_file = 'scratch/position_cleanup_results.xlsx'
    
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    engine = _engine()
    
    results = []
    total = len(df)
    for i, row in df.iterrows():
        print(f"Processing {i+1}/{total} - Payroll ID: {row.get('Payroll ID')}")
        result_row = await process_row(row.copy(), engine)
        results.append(result_row)
        # Give a small sleep to avoid overwhelming API or getting rate limited too hard
        await asyncio.sleep(0.5)
        
    engine.dispose()
    
    print(f"Saving results to {output_file}...")
    out_df = pd.DataFrame(results)
    out_df.to_excel(output_file, index=False)
    
    # Also copy it back to root
    import shutil
    shutil.copy(output_file, 'position_cleanup_results.xlsx')
    
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from apps.position_requests.scheduler import load_records, save_records
import httpx
import pypdf
import docx
import io
import os
import openai

router = APIRouter()
templates = Jinja2Templates(directory="templates")

ALLOWED_RECRUITERS = ["Mercedes", "Piyush"]

@router.get("", response_class=HTMLResponse)
async def position_requests_page(request: Request):
    records = load_records()
    user = request.session.get("user")
    
    # Sort by newest first (do not sort by completed status so items don't jump to bottom)
    records.sort(key=lambda r: r.get("received_date", ""), reverse=True)
    
    context = {
        "request": request,
        "user": user,
        "records": records,
        "recruiters": ALLOWED_RECRUITERS
    }
    return templates.TemplateResponse("apps/position_requests.html", context)

@router.post("/update")
async def update_position_request(
    request: Request,
    message_id: str = Form(...),
    recruiter: str = Form(""),
    completed: str | None = Form(None)
):
    records = load_records()
    record = next((r for r in records if r.get("message_id") == message_id), None)
    
    if not record:
        return JSONResponse({"status": "error", "message": "Record not found"}, status_code=404)
        
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS else ""
    record["completed"] = (completed is not None and completed != "")
    
    save_records(records)
    
    return JSONResponse({"status": "success", "record": record})

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
            
            # If the response is an HTML page, it means Jotform blocked the download with a login screen
            if content.strip().lower().startswith(b"<!doc") or content.strip().lower().startswith(b"<html"):
                return (
                    "JotForm Privacy Error: The file is blocked by a login screen.\n\n"
                    "To fix this, go to your JotForm Account Settings -> Security, "
                    "and UNCHECK the option 'Require log-in to view uploaded files'."
                )
                
            url_lower = url.lower()
            if ".pdf" in url_lower:
                pdf_reader = pypdf.PdfReader(io.BytesIO(content))
                text = []
                for page in pdf_reader.pages:
                    if page.extract_text():
                        text.append(page.extract_text())
                return "\n".join(text)
            elif ".docx" in url_lower:
                doc = docx.Document(io.BytesIO(content))
                text = [para.text for para in doc.paragraphs]
                return "\n".join(text)
            else:
                try:
                    return content.decode("utf-8")
                except:
                    return "Unsupported file format for text extraction. Download it manually."
    except Exception as e:
        return f"Error extracting resume text: {str(e)}"

@router.post("/analyze")
async def analyze_request(
    request: Request,
    message_id: str = Form(...)
):
    records = load_records()
    record = next((r for r in records if r.get("message_id") == message_id), None)
    
    if not record:
        return JSONResponse({"status": "error", "message": "Record not found"}, status_code=404)
        
    resume_url = record.get("resume_link", "")
    experience_text = record.get("experience", "")
    requested_positions = record.get("positions", "")
    
    resume_text = await extract_text_from_url(resume_url) if resume_url else "No resume attached."
    
    combined_text = f"User provided experience:\n{experience_text}\n\nResume text:\n{resume_text}"
    prompt = f"""
The candidate is applying for the following requested positions: {requested_positions}

Here is the candidate's experience information:
{combined_text}

Analyze the candidate's experience and determine if they are qualified for the requested positions. Give a clear, concise summary of your findings.
"""
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400
        )
        ai_analysis = response.choices[0].message.content
    except Exception as e:
        ai_analysis = f"AI Analysis Error: {str(e)}"

    return JSONResponse({
        "status": "success",
        "resume_text": resume_text,
        "ai_analysis": ai_analysis
    })

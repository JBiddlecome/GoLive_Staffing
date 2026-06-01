from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from apps.position_requests.scheduler import load_records, save_records, _engine
from sqlalchemy import text
import re
import httpx
import pypdf
import docx
import io
import os
import openai
from apps.position_requests.scheduler import ai_analyze as scheduler_ai_analyze

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
    
    status, ai_analysis, approved_positions = await scheduler_ai_analyze(
        resume_text, experience_text, requested_positions
    )

    return JSONResponse({
        "status": "success",
        "resume_text": resume_text,
        "ai_analysis": ai_analysis,
        "eval_status": status,
        "approved_positions": approved_positions,
    })

from apps.position_requests.utils import add_position_to_employee

@router.get("/automation-status")
async def get_automation_status(request: Request):
    from apps.position_requests.scheduler import _resolve_data_dir
    import json
    settings_file = _resolve_data_dir() / "position_requests_settings.json"
    auto_add = False
    if settings_file.exists():
        try:
            settings = json.loads(settings_file.read_text())
            auto_add = settings.get("auto_add_approved", False)
        except:
            pass
    return JSONResponse({"auto_add_approved": auto_add})

@router.post("/toggle-automation")
async def toggle_automation(request: Request, enabled: str = Form(...)):
    from apps.position_requests.scheduler import _resolve_data_dir
    import json
    settings_file = _resolve_data_dir() / "position_requests_settings.json"
    
    is_enabled = enabled.lower() == 'true'
    
    settings = {}
    if settings_file.exists():
        try:
            settings = json.loads(settings_file.read_text())
        except:
            pass
            
    settings["auto_add_approved"] = is_enabled
    
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    settings_file.write_text(json.dumps(settings, indent=2))
    
    return JSONResponse({"status": "success", "auto_add_approved": is_enabled})

@router.post("/add-position")
async def add_position(
    request: Request,
    phone: str = Form(...),
    positions: str = Form(...)
):
    success, msg = add_position_to_employee(phone, positions)
    if success:
        return JSONResponse({"status": "success", "message": msg})
    else:
        # We can just return 400 for generic error to keep it simple, or 500 if database error
        status_code = 400
        if "Database error" in msg:
            status_code = 500
        elif "not found" in msg:
            status_code = 404
        return JSONResponse({"status": "error", "message": msg}, status_code=status_code)


def send_position_denied_email(employee_email: str, first_name: str, requested_positions: list, qualification_based: bool = False):
    sender_email = "golive@culinarystaffing.com"
    
    import requests
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret, employee_email]):
        print("Skipping email: Microsoft 365 OAuth credentials missing or employee email is empty.")
        return False
        
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    try:
        r = requests.post(token_url, data=token_data)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"Failed to authenticate with Microsoft Graph: {e}")
        return False

    subject = "Update on Your Position Request"
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #047857;">Position Request Update</h2>
        <p>Hi {first_name},</p>
        <p>Thank you for submitting your position request. Unfortunately, you have not been approved for the following position(s) based on {"Culinary Staffing qualification requirements" if qualification_based else "the uploaded resume and experience"}:</p>
        <ul>
    """
    for p in requested_positions:
        html_body += f"<li><strong>{p}</strong></li>"

    html_body += f"""
        </ul>
        {"" if qualification_based else "<p>If you feel this is in error, or if you have additional experience not reflected on your current resume, please update your resume and try again.</p>"}
        <p>Best regards,<br>The Culinary Staffing Team</p>
      </body>
    </html>
    """
    
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [
                {"emailAddress": {"address": employee_email}}
            ]
        },
        "saveToSentItems": "true"
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to send email via MS Graph: {e}")
        return False

@router.post("/deny-position")
async def deny_position(
    request: Request,
    phone: str = Form(...),
    positions: str = Form(...),
    reason: str = Form("experience"),
    name: str = Form("")
):
    clean_phone = re.sub(r'\D', '', phone) if phone else ""
    if len(clean_phone) > 10:
        clean_phone = clean_phone[-10:]
        
    engine = _engine()
    try:
        with engine.connect() as conn:
            emp_res = None
            
            # 1. Try phone number lookup if valid
            if clean_phone and len(clean_phone) >= 10:
                emp_sql = text("""
                    SELECT employee_id, first_name, email 
                    FROM employee 
                    WHERE REPLACE(REPLACE(REPLACE(REPLACE(mobile, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                       OR REPLACE(REPLACE(REPLACE(REPLACE(home, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                       OR REPLACE(REPLACE(REPLACE(REPLACE(work, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                    LIMIT 1
                """)
                emp_res = conn.execute(emp_sql, {"phone": f"%{clean_phone}%"}).fetchone()
            
            # 2. Try name lookup if phone lookup didn't yield a match
            if not emp_res and name and name != "Unknown":
                name_sql = text("""
                    SELECT employee_id, first_name, email 
                    FROM employee 
                    WHERE CONCAT(first_name, ' ', last_name) = :name
                      AND status IN (1, 3, 6, 10, 14)
                """)
                name_results = conn.execute(name_sql, {"name": name}).fetchall()
                if len(name_results) > 1:
                    return JSONResponse({"status": "error", "message": "There are more than one user with this name in GoLive."}, status_code=400)
                elif len(name_results) == 1:
                    emp_res = name_results[0]
            
            if not emp_res:
                if not clean_phone or len(clean_phone) < 10:
                    return JSONResponse({"status": "error", "message": "No valid 10-digit phone number provided and employee not found by name."}, status_code=400)
                return JSONResponse({"status": "error", "message": "Employee not found in database by phone or name."}, status_code=404)
                
            first_name = emp_res[1] or "Employee"
            employee_email = emp_res[2]
            
            position_names = [p.strip() for p in positions.split(",") if p.strip()]
            if not position_names:
                position_names = [p.strip() for p in positions.split("\\n") if p.strip()]
            if not position_names:
                 position_names = [positions.strip()]
 
            msg = f"Successfully processed denial for positions: {', '.join(position_names)}"
            
            email_sent = False
            if employee_email:
                email_sent = send_position_denied_email(employee_email, first_name, position_names, qualification_based=(reason == "qualification"))
                if email_sent:
                    msg += ". Notification email sent."
                else:
                    msg += ". Failed to send email."
            else:
                msg += ". No email address found for employee."
                
            return JSONResponse({"status": "success", "message": msg})
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"Database error: {str(e)}"}, status_code=500)
    finally:
        engine.dispose()

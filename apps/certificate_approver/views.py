from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
import boto3
import os
import openai
import json
import requests
import fitz
import base64
from datetime import datetime
from apps.position_requests.scheduler import _engine  # Re-use DB connection logic

router = APIRouter()
templates = Jinja2Templates(directory="templates")

S3_BUCKET = os.getenv("S3_BUCKET", "web-application-files")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
CERTIFICATE_APPROVER_USER_ID = 35598

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=S3_REGION
    )

def _to_date(val):
    """Safely convert a DB date value (date, datetime, or string) to a date object."""
    if val is None:
        return None
    if hasattr(val, 'date'):
        return val.date()
    try:
        from datetime import date
        if isinstance(val, str):
            return datetime.strptime(val[:10], '%Y-%m-%d').date()
        return val
    except Exception:
        return None


def get_pending_certificates():
    import urllib.parse
    engine = _engine()
    pending = []
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT ec.id, ec.employee_id, ec.certification_id, c.name AS cert_type_name,
                       ec.file, ec.number, ec.issued_at, ec.expires_at,
                       e.first_name, e.last_name, e.email,
                       e.start_date, e.start_date2, e.status,
                       c.other_work_type_id,
                       owt.name          AS owt_name,
                       owt.work_hours    AS owt_work_hours,
                       owt.non_work_hours AS owt_non_work_hours,
                       owt.rate          AS owt_rate,
                       owt.custom_rate   AS owt_custom_rate,
                       owt.cost          AS owt_cost,
                       owt.cost_description AS owt_cost_description
                FROM employee_certification ec
                JOIN employee e ON ec.employee_id = e.employee_id
                JOIN certification c ON ec.certification_id = c.id
                LEFT JOIN other_work_type owt ON c.other_work_type_id = owt.id
                    AND owt.deleted_at IS NULL
                WHERE ec.approved_at IS NULL
                  AND ec.file IS NOT NULL
                  AND e.email NOT LIKE '%[DELETED]%'
            """)
            res = conn.execute(query).fetchall()
            for row in res:
                file_name = row.file
                safe_file_name = urllib.parse.quote(file_name)
                cert_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/employee/certification/{safe_file_name}"

                issued_date = _to_date(row.issued_at)
                effective_start = _to_date(row.start_date2) if row.start_date2 else _to_date(row.start_date)
                other_work_eligible = bool(
                    row.other_work_type_id
                    and issued_date is not None
                    and effective_start is not None
                    and issued_date > effective_start
                    and row.status != 6
                )

                pending.append({
                    "id": row.id,
                    "employee_id": row.employee_id,
                    "cert_type_id": row.certification_id,
                    "cert_type_name": row.cert_type_name,
                    "file_name": file_name,
                    "number": row.number,
                    "issued_at": str(row.issued_at) if row.issued_at else None,
                    "expires_at": str(row.expires_at) if row.expires_at else None,
                    "first_name": row.first_name or "Unknown",
                    "last_name": row.last_name or "",
                    "email": row.email,
                    "cert_url": cert_url,
                    "other_work_type_id": row.other_work_type_id,
                    "other_work_eligible": other_work_eligible,
                    "owt_name": row.owt_name,
                    "owt_work_hours": float(row.owt_work_hours) if row.owt_work_hours is not None else None,
                    "owt_non_work_hours": float(row.owt_non_work_hours) if row.owt_non_work_hours is not None else None,
                    "owt_rate": row.owt_rate,
                    "owt_custom_rate": float(row.owt_custom_rate) if row.owt_custom_rate is not None else None,
                    "owt_cost": float(row.owt_cost) if row.owt_cost is not None else None,
                    "owt_cost_description": row.owt_cost_description,
                })
            return pending
    except Exception as e:
        print(f"Error fetching pending certs: {e}")
        return []

def get_approved_certificates_for_test():
    import urllib.parse
    engine = _engine()
    test_certs = []
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT ec.id, ec.employee_id, ec.certification_id, c.name AS cert_type_name,
                       ec.file, ec.number, ec.issued_at, ec.expires_at,
                       e.first_name, e.last_name, e.email,
                       e.start_date, e.start_date2, e.status,
                       c.other_work_type_id,
                       owt.name          AS owt_name,
                       owt.work_hours    AS owt_work_hours,
                       owt.non_work_hours AS owt_non_work_hours,
                       owt.rate          AS owt_rate,
                       owt.custom_rate   AS owt_custom_rate,
                       owt.cost          AS owt_cost,
                       owt.cost_description AS owt_cost_description
                FROM (
                    SELECT id, employee_id, certification_id, file, number, issued_at, expires_at, approved_at,
                           ROW_NUMBER() OVER(PARTITION BY certification_id ORDER BY id DESC) as rn
                    FROM employee_certification
                    WHERE approved_at IS NOT NULL
                      AND file IS NOT NULL
                      AND certification_id IN (3, 10, 14, 17, 18, 20, 29, 37, 40, 43, 44, 50, 51, 52, 54, 55, 56, 59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 73)
                ) ec
                JOIN employee e ON ec.employee_id = e.employee_id
                JOIN certification c ON ec.certification_id = c.id
                LEFT JOIN other_work_type owt ON c.other_work_type_id = owt.id AND owt.deleted_at IS NULL
                WHERE ec.rn <= 10
                  AND e.email NOT LIKE '%[DELETED]%'
                ORDER BY ec.certification_id, ec.id DESC
            """)
            res = conn.execute(query).fetchall()
            for row in res:
                file_name = row.file
                safe_file_name = urllib.parse.quote(file_name)
                cert_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/employee/certification/{safe_file_name}"

                issued_date = _to_date(row.issued_at)
                effective_start = _to_date(row.start_date2) if row.start_date2 else _to_date(row.start_date)
                other_work_eligible = bool(
                    row.other_work_type_id
                    and issued_date is not None
                    and effective_start is not None
                    and issued_date > effective_start
                    and row.status != 6
                )

                test_certs.append({
                    "id": row.id,
                    "employee_id": row.employee_id,
                    "cert_type_id": row.certification_id,
                    "cert_type_name": row.cert_type_name,
                    "file_name": file_name,
                    "number": row.number,
                    "issued_at": str(row.issued_at) if row.issued_at else None,
                    "expires_at": str(row.expires_at) if row.expires_at else None,
                    "first_name": row.first_name or "Unknown",
                    "last_name": row.last_name or "",
                    "email": row.email,
                    "cert_url": cert_url,
                    "other_work_type_id": row.other_work_type_id,
                    "other_work_eligible": other_work_eligible,
                    "owt_name": row.owt_name,
                    "owt_work_hours": row.owt_work_hours,
                    "owt_non_work_hours": row.owt_non_work_hours,
                    "owt_rate": row.owt_rate,
                    "owt_custom_rate": row.owt_custom_rate,
                    "owt_cost": row.owt_cost,
                    "owt_cost_description": row.owt_cost_description
                })
            return test_certs
    except Exception as e:
        print(f"Error fetching test certs: {e}")
        return []

@router.get("/", response_class=HTMLResponse)
async def certificate_approver_dashboard(request: Request):
    from apps.certificate_approver.scheduler import get_auto_approve_enabled
    
    user = request.session.get("user")
    pending = get_pending_certificates()
        
    context = {
        "request": request,
        "user": user,
        "pending_certificates": pending,
        "auto_approve_enabled": get_auto_approve_enabled()
    }
    return templates.TemplateResponse("apps/certificate_approver.html", context)

@router.post("/api/toggle-auto-approve")
async def toggle_auto_approve(request: Request):
    from apps.certificate_approver.scheduler import get_auto_approve_enabled, set_auto_approve_enabled
    data = await request.json()
    enabled = data.get("enabled", False)
    set_auto_approve_enabled(enabled)
    return JSONResponse({"status": "success", "enabled": get_auto_approve_enabled()})

@router.get("/test", response_class=HTMLResponse)
async def certificate_approver_test_dashboard(request: Request):
    from apps.certificate_approver.scheduler import get_auto_approve_enabled
    test_certificates = get_approved_certificates_for_test()
    return templates.TemplateResponse(
        "apps/certificate_approver_test.html",
        {
            "request": request,
            "test_certificates": test_certificates,
            "auto_approve_enabled": get_auto_approve_enabled()
        }
    )

def get_ai_prompt():
    prompt_path = os.path.join(os.getcwd(), "certificate_approval.md")
    try:
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print("Error reading certificate_approval.md:", e)
        return "You are an expert certificate verifier."

async def analyze_certificate_ai(cert_url: str, cert_type_id: int, cert_type_name: str,
                                 issued_at: str, expires_at: str, number: str, file_name: str,
                                 employee_name: str = ""):
    try:
        prompt = get_ai_prompt()

        system_content = "You are a certificate verification assistant for GoLive! Staffing. Follow the rules in the provided specification."

        # Pre-load the document (PDF or Image) to process it locally
        # This allows us to lower the resolution and save on AI API tokens
        is_pdf = cert_url.lower().split('?')[0].endswith('.pdf')
        file_doc = None
        try:
            res = requests.get(cert_url)
            if res.status_code == 200:
                file_doc = fitz.open(stream=res.content, filetype="pdf" if is_pdf else None)
        except Exception as e:
            print("Failed to pre-load user submission:", e)

        if is_pdf and file_doc is not None:
            file_format_line = f"Original File Format: PDF ({len(file_doc)} pages — rendered below as images for AI review)"
        elif is_pdf:
            file_format_line = "Original File Format: PDF (page count unavailable — rendering failed)"
        else:
            file_format_line = f"Original File Format: IMAGE ({file_name.rsplit('.', 1)[-1].upper() if '.' in file_name else 'unknown'})"

        user_message_text = f"""
Here is the specification document:
{prompt}

We are evaluating the following uploaded document:
cert_type_id: {cert_type_id}
cert_type_name: {cert_type_name}
File Name: {file_name}
{file_format_line}
Claimed Issue Date: {issued_at or datetime.now().strftime('%Y-%m-%d')}
Claimed Expiration Date: {expires_at or 'N/A'}
Claimed Number: {number or 'N/A'}
Employee Name on File: {employee_name or 'N/A'}

IMPORTANT: All documents in this system are rendered as images before being sent for AI review — including PDFs. Use the "Original File Format" field above (not the visual format of the images you receive) to determine the file type and page count.

Please verify the attached pages (The User Submission) according to the rules for this cert_type_id.
"""

        content = [
            {"type": "text", "text": user_message_text},
            {"type": "text", "text": "--- START OF USER SUBMISSION ---"}
        ]

        if file_doc is not None:
            for page_num, page in enumerate(file_doc):
                # Use a low DPI (50) to dramatically reduce image resolution and token consumption
                pix = page.get_pixmap(dpi=50)
                b64 = base64.b64encode(pix.tobytes("png")).decode('utf-8')
                if is_pdf:
                    content.append({"type": "text", "text": f"Page {page_num + 1} of {len(file_doc)}:"})
                # Add detail: low to ensure minimum token cost
                content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "low"}})
        else:
            # Fallback if fitz failed
            content.append({"type": "image_url", "image_url": {"url": cert_url, "detail": "low"}})
            
        content.append({"type": "text", "text": "--- END OF USER SUBMISSION ---"})
        
        # Check for baseline image
        baseline_dir = os.path.join(os.getcwd(), "apps", "certificate_approver", "baselines", str(cert_type_id))
        if os.path.exists(baseline_dir):
            files = [f for f in os.listdir(baseline_dir) if os.path.isfile(os.path.join(baseline_dir, f))]
            if files:
                baseline_path = os.path.join(baseline_dir, files[0])
                content.append({"type": "text", "text": "For your reference, here is an example of what the valid document should look like (baseline):"})
                content.append({"type": "text", "text": "--- START OF BASELINE REFERENCE ---"})
                
                if files[0].lower().endswith('.pdf'):
                    try:
                        doc = fitz.open(baseline_path)
                        for page_num, page in enumerate(doc):
                            pix = page.get_pixmap(dpi=50)
                            b64 = base64.b64encode(pix.tobytes("png")).decode('utf-8')
                            content.append({"type": "text", "text": f"Baseline Page {page_num + 1}:"})
                            content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "low"}})
                    except Exception as e:
                        print("Failed to parse PDF baseline:", e)
                else:
                    try:
                        doc = fitz.open(baseline_path)
                        for page_num, page in enumerate(doc):
                            pix = page.get_pixmap(dpi=50)
                            b64 = base64.b64encode(pix.tobytes("png")).decode('utf-8')
                            content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "low"}})
                    except Exception as e:
                        print("Failed to parse IMAGE baseline:", e)
                
                content.append({"type": "text", "text": "--- END OF BASELINE REFERENCE ---"})

        client = openai.AsyncOpenAI(api_key=os.getenv("CERTIFICATE_APPROVER") or os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_PRODUCTION", "gpt-4o"),
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": content}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        resp_text = response.choices[0].message.content or "{}"
        result = json.loads(resp_text)
        return {"status": "success", "ai_analysis": result}
    except Exception as e:
        return {"status": "error", "message": f"AI Error: {str(e)}"}

@router.post("/analyze")
async def analyze_cert(
    request: Request,
    cert_url: str = Form(...),
    cert_type_id: int = Form(...),
    cert_type_name: str = Form(""),
    issued_at: str = Form(""),
    expires_at: str = Form(""),
    number: str = Form(""),
    file_name: str = Form(...),
    first_name: str = Form(""),
    last_name: str = Form("")
):
    from apps.api_usage_tracker import log_api_usage
    log_api_usage("Certificate Approver", request=request)
    
    employee_name = f"{first_name} {last_name}".strip()
    result = await analyze_certificate_ai(cert_url, cert_type_id, cert_type_name, issued_at, expires_at, number, file_name, employee_name)
    if result["status"] == "success":
        return JSONResponse(result)
    else:
        return JSONResponse(result, status_code=500)

def send_notification_email(employee_email: str, first_name: str, status: str, cert_type_name: str, reason: str = ""):
    sender_email = "golive@culinarystaffing.com"
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

    if status == "Approved":
        subject = f"Certificate Approved: {cert_type_name}"
        body_text = f"Hi {first_name},<br><br>Good news! Your uploaded certificate for <b>{cert_type_name}</b> has been approved and is now active on your account."
    else:
        subject = f"Certificate Declined: {cert_type_name}"
        body_text = f"Hi {first_name},<br><br>Unfortunately, your uploaded certificate for <b>{cert_type_name}</b> was declined.<br><br><b>Reason:</b> {reason}<br><br>Please review and upload a valid document."

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #047857;">Certificate Update</h2>
        <p>{body_text}</p>
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

def _calculate_other_work_rate(conn, employee_id: int, owt_rate: str, owt_custom_rate, date_str: str) -> float:
    """Calculate the pay rate for an other_work_type entry using the same logic as GoLive."""
    try:
        if owt_rate == 'CUSTOM':
            return float(owt_custom_rate or 0)

        elif owt_rate == 'MINIMUM_WAGE':
            row = conn.execute(text("""
                SELECT mwra.rate
                FROM employee e
                JOIN min_wage_rate_amount mwra ON mwra.min_wage_id = e.min_wage_id
                WHERE e.employee_id = :eid
                  AND (mwra.start_date IS NULL OR mwra.start_date <= :date)
                  AND (mwra.end_date IS NULL OR mwra.end_date >= :date)
                ORDER BY mwra.start_date DESC
                LIMIT 1
            """), {"eid": employee_id, "date": date_str}).fetchone()
            return float(row.rate) if row and row.rate else 0.0

        elif owt_rate == '91_DAYS_SHIFTS':
            from datetime import timedelta
            try:
                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
            except Exception:
                date_obj = datetime.now()
            # Align to the most recent Sunday on or before the date (matches GoLive logic)
            days_since_sunday = (date_obj.weekday() + 1) % 7
            last_sunday = date_obj - timedelta(days=days_since_sunday)
            start_date = last_sunday - timedelta(days=91)

            row = conn.execute(text("""
                SELECT SUM(se.rate) AS total_rate, COUNT(*) AS quantity
                FROM shift_employee se
                INNER JOIN event ev ON ev.event_id = se.event_id
                WHERE se.employee_id = :eid
                  AND ev.date >= :start_date
                  AND ev.date <= :end_date
                  AND se.cancel_reason = 0
                  AND se.confirmed = 1
                  AND se.deleted_at IS NULL
                  AND EXISTS (
                      SELECT 1 FROM timesheet ts
                      WHERE ts.shift_employee_id = se.shift_employee_id
                  )
            """), {
                "eid": employee_id,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": last_sunday.strftime('%Y-%m-%d'),
            }).fetchone()

            if row and row.quantity:
                return round(float(row.total_rate) / float(row.quantity), 2)
            return 0.0

    except Exception as e:
        print(f"Error calculating other work rate: {e}")

    return 0.0


def register_other_work_for_cert(employee_id: int, other_work_type_id: int, issued_at_str: str) -> tuple:
    """
    Insert an employee_other_work record when a certificate is approved.
    Mirrors GoLive's EmployeeOtherWork::registerBy() — skips duplicates silently.
    Date used is the certificate's issued_at (when the employee actually completed the work).
    """
    try:
        date_str = issued_at_str[:10] if issued_at_str and len(issued_at_str) >= 10 else datetime.now().strftime('%Y-%m-%d')

        engine = _engine()
        try:
            with engine.begin() as conn:
                owt = conn.execute(text("""
                    SELECT id, work_hours, non_work_hours, rate, custom_rate, cost
                    FROM other_work_type
                    WHERE id = :owt_id AND deleted_at IS NULL
                    LIMIT 1
                """), {"owt_id": other_work_type_id}).fetchone()

                if not owt:
                    return False, f"Other work type {other_work_type_id} not found or deleted"

                # Duplicate guard — same logic as GoLive's registerBy()
                existing = conn.execute(text("""
                    SELECT id FROM employee_other_work
                    WHERE employee_id = :eid
                      AND other_work_type_id = :owt_id
                      AND date = :date
                    LIMIT 1
                """), {"eid": employee_id, "owt_id": other_work_type_id, "date": date_str}).fetchone()

                if existing:
                    return True, "already_exists"

                rate = _calculate_other_work_rate(conn, employee_id, owt.rate, owt.custom_rate, date_str)

                conn.execute(text("""
                    INSERT INTO employee_other_work
                        (employee_id, other_work_type_id, work_hours, non_work_hours, rate, cost, date, notes, created_at)
                    VALUES
                        (:eid, :owt_id, :work_hours, :non_work_hours, :rate, :cost, :date, :notes, NOW())
                """), {
                    "eid": employee_id,
                    "owt_id": other_work_type_id,
                    "work_hours": float(owt.work_hours or 0),
                    "non_work_hours": float(owt.non_work_hours or 0),
                    "rate": rate,
                    "cost": float(owt.cost or 0),
                    "date": date_str,
                    "notes": "Auto-registered via GoLive Staffing Tools — Certificate Approver",
                })

            return True, ""
        finally:
            engine.dispose()

    except Exception as e:
        print(f"Error registering other work: {e}")
        return False, str(e)


def approve_cert_action(record_id: int, first_name: str, email: str, cert_type_name: str, issued_at: str = "", expires_at: str = "", number: str = ""):
    try:
        engine = _engine()
        try:
            with engine.begin() as conn:
                record = conn.execute(text("SELECT employee_id, certification_id FROM employee_certification WHERE id = :record_id"), {"record_id": record_id}).fetchone()
                
                update_sql = "UPDATE employee_certification SET approved_at = NOW(), approved_by = :approved_by"
                params = {"record_id": record_id, "approved_by": CERTIFICATE_APPROVER_USER_ID}
                
                if issued_at:
                    update_sql += ", issued_at = :issued_at"
                    params["issued_at"] = issued_at
                    
                if expires_at:
                    update_sql += ", expires_at = :expires_at"
                    params["expires_at"] = expires_at
                    
                if number:
                    update_sql += ", number = :number"
                    params["number"] = number
                    
                update_sql += " WHERE id = :record_id"
                conn.execute(text(update_sql), params)
                
                if record:
                    emp_id = record.employee_id
                    cert_id = record.certification_id
                    is_food_handler = cert_id in [3, 17, 18, 20, 50, 55, 60, 66] or "food handler" in cert_type_name.lower() or "food protection" in cert_type_name.lower() or "food worker" in cert_type_name.lower()
                    
                    if is_food_handler:
                        conn.execute(text("""
                            UPDATE employee_certification 
                            SET approved_at = NOW(), approved_by = :approved_by 
                            WHERE employee_id = :emp_id 
                              AND certification_id = 45 
                              AND approved_at IS NULL
                        """), {"emp_id": emp_id, "approved_by": CERTIFICATE_APPROVER_USER_ID})
        finally:
            engine.dispose()
            
        if email:
            send_notification_email(email, first_name, "Approved", cert_type_name)
            
        return True, ""
    except Exception as e:
        return False, str(e)

@router.post("/approve")
async def approve_cert(
    request: Request,
    record_id: int = Form(...),
    first_name: str = Form(""),
    email: str = Form(""),
    cert_type_name: str = Form(""),
    register_other_work: str = Form("false"),
    employee_id: int = Form(0),
    other_work_type_id: int = Form(0),
    issued_at: str = Form(""),
    expires_at: str = Form(""),
    number: str = Form(""),
):
    success, err = approve_cert_action(record_id, first_name, email, cert_type_name, issued_at, expires_at, number)
    if not success:
        return JSONResponse({"status": "error", "message": err}, status_code=500)

    ow_message = None
    if register_other_work == "true" and employee_id and other_work_type_id:
        ow_success, ow_err = register_other_work_for_cert(employee_id, other_work_type_id, issued_at)
        if not ow_success:
            ow_message = f"Certificate approved but Other Work registration failed: {ow_err}"
        elif ow_err == "already_exists":
            ow_message = "Other Work already registered for this employee/type/date — skipped duplicate."

    return JSONResponse({"status": "success", "ow_message": ow_message})

def deny_cert_action(record_id: int, file_name: str, first_name: str, email: str, cert_type_name: str, reason: str):
    try:
        s3 = get_s3_client()
        
        # 1. Delete S3 object
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=f'employee/certification/{file_name}')
        except Exception as e:
            print(f"Error deleting from S3: {e}")
            
        # 2. Delete from DB
        engine = _engine()
        try:
            with engine.begin() as conn:
                delete_sql = text("DELETE FROM employee_certification WHERE id = :record_id")
                conn.execute(delete_sql, {"record_id": record_id})
        finally:
            engine.dispose()
            
        # 3. Send Email
        if email:
            send_notification_email(email, first_name, "Denied", cert_type_name, reason)
            
        return True, ""
    except Exception as e:
        return False, str(e)

@router.post("/deny")
async def deny_cert(
    request: Request,
    record_id: int = Form(...),
    file_name: str = Form(...),
    first_name: str = Form(""),
    email: str = Form(""),
    cert_type_name: str = Form(""),
    reason: str = Form("")
):
    success, err = deny_cert_action(record_id, file_name, first_name, email, cert_type_name, reason)
    if success:
        return JSONResponse({"status": "success"})
    else:
        return JSONResponse({"status": "error", "message": err}, status_code=500)

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
import boto3
import os
import openai
import json
import requests
from apps.position_requests.scheduler import _engine  # Re-use DB connection logic

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "web-application-files")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=S3_REGION
    )

def get_pending_photos():
    engine = _engine()
    pending_photos = []
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT t.related_id as employee_id, t.file_name, e.first_name, e.last_name, e.email 
                FROM temporary_file t
                JOIN employee e ON t.related_id = e.employee_id
                WHERE t.type = 'EMPLOYEE_PHOTO' AND t.file_name NOT LIKE '%[DELETED]%'
            """)
            res = conn.execute(query).fetchall()
            for row in res:
                employee_id, file_name, first_name, last_name, email = row
                photo_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/temporary/{file_name}"
                pending_photos.append({
                    "employee_id": employee_id,
                    "file_name": file_name,
                    "first_name": first_name or "Unknown",
                    "last_name": last_name or "",
                    "email": email,
                    "photo_url": photo_url
                })
    except Exception as e:
        print(f"Error fetching pending photos: {e}")
    finally:
        engine.dispose()
    return pending_photos

@router.get("", response_class=HTMLResponse)
async def profile_picture_approval_page(request: Request):
    from apps.profile_picture_approval.scheduler import get_auto_approve_enabled
    
    user = request.session.get("user")
    pending_photos = get_pending_photos()
        
    context = {
        "request": request,
        "user": user,
        "pending_photos": pending_photos,
        "auto_approve_enabled": get_auto_approve_enabled()
    }
    return templates.TemplateResponse("apps/profile_picture_approval.html", context)

@router.post("/api/toggle-auto-approve")
async def toggle_auto_approve(request: Request):
    from apps.profile_picture_approval.scheduler import get_auto_approve_enabled, set_auto_approve_enabled
    
    data = await request.json()
    enabled = data.get("enabled", False)
    set_auto_approve_enabled(enabled)
    return JSONResponse({"status": "success", "enabled": get_auto_approve_enabled()})

AI_PROMPT = """
You are an expert Image Content Moderator. Your task is to evaluate a user's uploaded profile picture for suitability based on strict criteria.

Evaluation Criteria:
Face Position: The person should be generally facing forward. It is okay if they are slightly facing to the side, but deny the picture if they are turned sideways 45 degrees or more. Obscured views are not allowed.
Clarity & Distance: The face must be close to the camera and in focus.
No Accessories: No sunglasses or masks. Hats are acceptable as long as they do not cover the eyes. VERY IMPORTANT regarding glasses: If you can clearly see the person's eyes through the lenses, they MUST be considered prescription glasses and NOT sunglasses. Do not falsely reject prescription glasses as sunglasses. These are perfectly okay to approve.
No Filters: No "beauty" filters, AR ears/noses, or heavy digital distortions.
Safety & Ethics: Strictly reject any photo containing:
Nudity or suggestive content.
Hate symbols or racist imagery (e.g., swastikas, white supremacist symbols).
Rude or offensive gestures (e.g., middle fingers).
Violence or weapons.

Output Format: Provide a JSON response with the following keys:
{
  "suitable": true | false,
  "reason": "A concise, polite explanation if suitable is false (e.g., 'Please remove your sunglasses')",
  "confidence": 0.95
}
"""

async def analyze_photo_ai(photo_url: str):
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_PRODUCTION", "gpt-4o"),
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": AI_PROMPT},
                        {"type": "image_url", "image_url": {"url": photo_url}}
                    ],
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        content = response.choices[0].message.content or "{}"
        result = json.loads(content)
        return {"status": "success", "ai_analysis": result}
    except Exception as e:
        return {"status": "error", "message": f"AI Error: {str(e)}"}

@router.post("/analyze")
async def analyze_photo(request: Request, photo_url: str = Form(...)):
    result = await analyze_photo_ai(photo_url)
    if result["status"] == "success":
        return JSONResponse(result)
    else:
        return JSONResponse(result, status_code=500)

def send_notification_email(employee_email: str, first_name: str, status: str):
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

    small_print = ""
    if status == "Approved":
        subject = "Employee Photo Approved"
        body_text = f"Hi {first_name},<br><br>Good news! Your new profile picture has been approved and is now live on your account."
    else:
        subject = "Employee Photo Rejected"
        body_text = f"Hi {first_name},<br><br>Unfortunately, your profile picture upload was rejected because it did not meet our guidelines. Please upload a real, clear, close-up photo of yourself facing forward, without any face coverings."
        small_print = "<hr style='margin-top: 30px; border: none; border-top: 1px solid #eee;'><p style='font-size: 11px; color: #666;'><em>Profile pictures are initially monitored using AI. If you believe your uploaded photo is being denied incorrectly, you can email it to info@culinarystaffing.com for manual approval.</em></p>"

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #047857;">Profile Picture Update</h2>
        <p>{body_text}</p>
        <p>Best regards,<br>The Culinary Staffing Team</p>
        {small_print}
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

def approve_photo_action(employee_id: int, file_name: str, first_name: str, email: str):
    try:
        s3 = get_s3_client()
        
        # 1. Move S3 object
        copy_source = {'Bucket': S3_BUCKET, 'Key': f'temporary/{file_name}'}
        new_key = f'employee/photo/{file_name}'
        
        try:
            s3.copy_object(Bucket=S3_BUCKET, CopySource=copy_source, Key=new_key, ACL='public-read')
            s3.delete_object(Bucket=S3_BUCKET, Key=f'temporary/{file_name}')
        except Exception as e:
            return False, f"S3 Error: {str(e)}"
            
        # 2. Update DB
        engine = _engine()
        try:
            with engine.begin() as conn:
                update_sql = text("UPDATE employee SET photo = :file_name WHERE employee_id = :employee_id")
                conn.execute(update_sql, {"file_name": file_name, "employee_id": employee_id})
                
                delete_sql = text("DELETE FROM temporary_file WHERE type = 'EMPLOYEE_PHOTO' AND related_id = :employee_id")
                conn.execute(delete_sql, {"employee_id": employee_id})
        finally:
            engine.dispose()
            
        # 3. Send Email
        if email:
            send_notification_email(email, first_name, "Approved")
            
        return True, ""
    except Exception as e:
        return False, str(e)

@router.post("/approve")
async def approve_photo(
    request: Request,
    employee_id: int = Form(...),
    file_name: str = Form(...),
    first_name: str = Form(""),
    email: str = Form("")
):
    success, err = approve_photo_action(employee_id, file_name, first_name, email)
    if success:
        return JSONResponse({"status": "success"})
    else:
        return JSONResponse({"status": "error", "message": err}, status_code=500)

def deny_photo_action(employee_id: int, file_name: str, first_name: str, email: str):
    try:
        s3 = get_s3_client()
        
        # 1. Delete S3 object
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=f'temporary/{file_name}')
        except Exception as e:
            print(f"Error deleting from S3: {e}")
            
        # 2. Delete from DB
        engine = _engine()
        try:
            with engine.begin() as conn:
                delete_sql = text("DELETE FROM temporary_file WHERE type = 'EMPLOYEE_PHOTO' AND related_id = :employee_id")
                conn.execute(delete_sql, {"employee_id": employee_id})
        finally:
            engine.dispose()
            
        # 3. Send Email
        if email:
            send_notification_email(email, first_name, "Denied")
            
        return True, ""
    except Exception as e:
        return False, str(e)

@router.post("/deny")
async def deny_photo(
    request: Request,
    employee_id: int = Form(...),
    file_name: str = Form(...),
    first_name: str = Form(""),
    email: str = Form("")
):
    success, err = deny_photo_action(employee_id, file_name, first_name, email)
    if success:
        return JSONResponse({"status": "success"})
    else:
        return JSONResponse({"status": "error", "message": err}, status_code=500)

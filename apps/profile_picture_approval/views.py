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

@router.get("", response_class=HTMLResponse)
async def profile_picture_approval_page(request: Request):
    user = request.session.get("user")
    engine = _engine()
    pending_photos = []
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT t.related_id as employee_id, t.file_name, e.first_name, e.last_name, e.email 
                FROM temporary_file t
                JOIN employee e ON t.related_id = e.employee_id
                WHERE t.type = 'EMPLOYEE_PHOTO'
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
        
    context = {
        "request": request,
        "user": user,
        "pending_photos": pending_photos
    }
    return templates.TemplateResponse("apps/profile_picture_approval.html", context)

AI_PROMPT = """
You are an expert profile picture reviewer.
Determine if this profile photo is approved or denied based on the following rules:
- It must be a real photo with no filters.
- There must be only one person in the photo.
- The person must be facing forward.
- The person's face must not be covered (no sunglasses, no hats, etc.).
- There should be no rude or racist gestures.
- The picture must be close up like a passport photo (not far away).

Return your response as a valid JSON object with the following schema:
{
  "status": "Approved" | "Denied",
  "reasoning": "Brief explanation of why it was approved or denied based on the rules."
}
"""

@router.post("/analyze")
async def analyze_photo(request: Request, photo_url: str = Form(...)):
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_PRODUCTION", "gpt-4o"), # Use production vision model
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
        return JSONResponse({"status": "success", "ai_analysis": result})
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"AI Error: {str(e)}"}, status_code=500)

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

    if status == "Approved":
        subject = "Employee Photo Approved"
        body_text = f"Hi {first_name},<br><br>Good news! Your new profile picture has been approved and is now live on your account."
    else:
        subject = "Employee Photo Rejected"
        body_text = f"Hi {first_name},<br><br>Unfortunately, your profile picture upload was rejected because it did not meet our guidelines. Please upload a real, clear, close-up photo of yourself facing forward, without any face coverings."

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #047857;">Profile Picture Update</h2>
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

@router.post("/approve")
async def approve_photo(
    request: Request,
    employee_id: int = Form(...),
    file_name: str = Form(...),
    first_name: str = Form(""),
    email: str = Form("")
):
    try:
        s3 = get_s3_client()
        
        # 1. Move S3 object
        copy_source = {'Bucket': S3_BUCKET, 'Key': f'temporary/{file_name}'}
        new_key = f'employee/photo/{file_name}'
        
        try:
            s3.copy_object(Bucket=S3_BUCKET, CopySource=copy_source, Key=new_key)
            s3.delete_object(Bucket=S3_BUCKET, Key=f'temporary/{file_name}')
        except Exception as e:
            return JSONResponse({"status": "error", "message": f"S3 Error: {str(e)}"}, status_code=500)
            
        # 2. Update DB
        engine = _engine()
        with engine.begin() as conn:
            update_sql = text("UPDATE employee SET photo = :file_name WHERE employee_id = :employee_id")
            conn.execute(update_sql, {"file_name": file_name, "employee_id": employee_id})
            
            delete_sql = text("DELETE FROM temporary_file WHERE type = 'EMPLOYEE_PHOTO' AND related_id = :employee_id")
            conn.execute(delete_sql, {"employee_id": employee_id})
            
        # 3. Send Email
        if email:
            send_notification_email(email, first_name, "Approved")
            
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        if 'engine' in locals():
            engine.dispose()

@router.post("/deny")
async def deny_photo(
    request: Request,
    employee_id: int = Form(...),
    file_name: str = Form(...),
    first_name: str = Form(""),
    email: str = Form("")
):
    try:
        s3 = get_s3_client()
        
        # 1. Delete S3 object
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=f'temporary/{file_name}')
        except Exception as e:
            print(f"Error deleting from S3: {e}")
            
        # 2. Delete from DB
        engine = _engine()
        with engine.begin() as conn:
            delete_sql = text("DELETE FROM temporary_file WHERE type = 'EMPLOYEE_PHOTO' AND related_id = :employee_id")
            conn.execute(delete_sql, {"employee_id": employee_id})
            
        # 3. Send Email
        if email:
            send_notification_email(email, first_name, "Denied")
            
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        if 'engine' in locals():
            engine.dispose()

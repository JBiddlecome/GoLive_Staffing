from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import requests
import os

import openai
from pydantic import BaseModel, Field

router = APIRouter()
templates = Jinja2Templates(directory="templates")

class EmailAnalysis(BaseModel):
    is_professional: bool = Field(description="Whether the email is generally professional.")
    professional_score: int = Field(description="Score out of 10 for professionalism.")
    red_flags: list[str] = Field(description="Any inappropriate, passive-aggressive, or otherwise concerning remarks.")
    suggestions: str = Field(description="Suggestions on how to improve the email tone or content.")

@router.get("", response_class=HTMLResponse)
async def email_review_page(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/email_review.html", {"request": request, "user": user})

@router.post("/fetch")
async def fetch_emails(
    request: Request,
    employee_email: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    sent_to: str = Form(None)
):
    # Verify O365 credentials
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        return JSONResponse(status_code=500, content={"error": "Microsoft 365 OAuth credentials not configured in environment (O365_TENANT_ID, O365_CLIENT_ID, O365_CLIENT_SECRET)."})

    # 1. Acquire OAuth Access Token
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
        error_info = getattr(e, 'response', None)
        err_msg = error_info.text if error_info else str(e)
        return JSONResponse(status_code=500, content={"error": f"Failed to authenticate with Microsoft Graph: {err_msg}"})

    # 2. Query sent messages
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # We filter by sentItems folder for the given user within the date range
    # Time formats must be ISO 8601, e.g., 2026-04-01T00:00:00Z
    start_iso = f"{start_date}T00:00:00Z"
    end_iso = f"{end_date}T23:59:59Z"
    
    # Fetch from the user's sent items
    url = f"https://graph.microsoft.com/v1.0/users/{employee_email}/mailFolders/sentitems/messages"
    
    # Graph API OData filter
    filter_query = f"sentDateTime ge {start_iso} and sentDateTime le {end_iso}"
    
    params = {
        "$filter": filter_query,
        "$select": "id,subject,bodyPreview,body,sentDateTime,toRecipients",
        "$top": "100"
    }
    
    try:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        messages = res.json().get("value", [])
    except Exception as e:
        error_info = getattr(e, 'response', None)
        err_msg = error_info.text if error_info else str(e)
        if "Request_ResourceNotFound" in err_msg:
            return JSONResponse(status_code=404, content={"error": f"Mailbox not found for {employee_email} or missing Mail.Read permissions."})
        return JSONResponse(status_code=500, content={"error": f"Failed to query emails: {err_msg}"})

    # Optional Local filtering by 'sent_to'
    filtered_messages = []
    if sent_to:
        sent_to_lower = sent_to.lower()
        for msg in messages:
            recipients = msg.get("toRecipients", [])
            match = False
            for r in recipients:
                addr = r.get("emailAddress", {}).get("address", "").lower()
                if sent_to_lower in addr:
                    match = True
                    break
            if match:
                filtered_messages.append(msg)
    else:
        filtered_messages = messages

    # Parse and format the output
    results = []
    for m in filtered_messages:
        # Extract recipients safely
        to_list = [r.get("emailAddress", {}).get("address", "") for r in m.get("toRecipients", [])]
        to_str = ", ".join([addr for addr in to_list if addr])
        
        # Grab text content if possible
        body_content = m.get("body", {}).get("content", "")
        content_type = m.get("body", {}).get("contentType", "text")
        
        results.append({
            "id": m.get("id"),
            "subject": m.get("subject"),
            "sent_date": m.get("sentDateTime"),
            "to": to_str,
            "preview": m.get("bodyPreview", ""),
            "body": body_content,
            "content_type": content_type
        })
        
    # Sort by date descending
    results.sort(key=lambda x: x["sent_date"], reverse=True)

    return JSONResponse(content={"status": "success", "data": results})

@router.post("/analyze")
async def analyze_email(request: Request):
    from apps.api_usage_tracker import log_api_usage
    log_api_usage("Email Review", request=request)
    
    data = await request.json()
    email_text = data.get("email_text", "")
    
    if not email_text:
        return JSONResponse(status_code=400, content={"error": "No email text provided."})
        
    api_key = os.getenv("EMAIL_REVIEW") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse(status_code=500, content={"error": "OpenAI API Key not configured."})
        
    # Standard OpenAI ChatCompletion for structured response
    try:
        client = openai.AsyncOpenAI(api_key=api_key)
        
        system_prompt = """
        You are an expert HR communications evaluator. Analyze this sent email for professionalism.
        
        IMPORTANT CRITERIA:
        Emails often contain quoted reply chains from other people below the sender's actual message. You MUST isolate and evaluate ONLY the new text originally authored by the Sender of this email. Do not penalize or evaluate any text that was written by other people further down in the thread.
        
        Your goals are:
        1. Determine if the sender's text is professional.
        2. Give the sender's text a professionalism score out of 10.
        3. Note any red flags (passive aggressiveness, toxicity, inappropriate language, policy violations) located specifically in the sender's text.
        4. Suggest a better, more professional rewrite or alternative if it has issues.
        
        Please return a strictly formatted JSON object with these exact keys:
        - "is_professional" (boolean)
        - "professional_score" (integer out of 10)
        - "red_flags" (list of strings)
        - "suggestions" (string)
        """
        
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Email Text:\n{email_text}"}
            ],
            temperature=0.2
        )
        
        result_str = response.choices[0].message.content
        import json
        result = json.loads(result_str)
        
        return JSONResponse(content={"status": "success", "analysis": result})
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/analyze_batch")
async def analyze_batch_email(request: Request):
    from apps.api_usage_tracker import log_api_usage
    log_api_usage("Email Review", request=request)
    
    data = await request.json()
    emails_data = data.get("emails", [])
    
    if not emails_data:
        return JSONResponse(status_code=400, content={"error": "No email text provided."})
        
    api_key = os.getenv("EMAIL_REVIEW") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse(status_code=500, content={"error": "OpenAI API Key not configured."})
        
    combined_emails_text = "Here are the emails to analyze:\n\n"
    for i, email_text in enumerate(emails_data):
        combined_emails_text += f"---\nEmail {i+1}:\n{email_text}\n"
    combined_emails_text += "\n---"

    try:
        client = openai.AsyncOpenAI(api_key=api_key)
        
        system_prompt = """
        You are an expert HR communications evaluator. Analyze this batch of sent emails from an employee for professionalism and overall tone.
        
        IMPORTANT CRITERIA:
        These emails often contain quoted reply chains from other people below the sender's actual messages. You MUST isolate and evaluate ONLY the text authored by the Sender. Do not penalize or evaluate any text that was written by other people further down in the thread.
        
        Your goals are:
        1. Determine if the sender's general communication style is professional.
        2. Give the sender an overall professionalism score out of 10.
        3. Note any red flags across the sender's text (passive aggressiveness, toxicity, inappropriate language, policy violations).
        4. Suggest a better, more professional approach or general communication coaching if there are issues.
        
        Please return a strictly formatted JSON object with these exact keys:
        - "is_professional" (boolean)
        - "professional_score" (integer out of 10)
        - "red_flags" (list of strings)
        - "suggestions" (string)
        """
        
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": combined_emails_text}
            ],
            temperature=0.2
        )
        
        result_str = response.choices[0].message.content
        import json
        result = json.loads(result_str)
        
        return JSONResponse(content={"status": "success", "analysis": result})
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/custom_query")
async def custom_query(request: Request):
    from apps.api_usage_tracker import log_api_usage
    log_api_usage("Email Review", request=request)
    
    data = await request.json()
    emails_data = data.get("emails", [])
    custom_prompt = data.get("custom_prompt", "")
    
    if not emails_data:
        return JSONResponse(status_code=400, content={"error": "No email text provided."})
    if not custom_prompt:
        return JSONResponse(status_code=400, content={"error": "No custom prompt provided."})
        
    api_key = os.getenv("EMAIL_REVIEW") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return JSONResponse(status_code=500, content={"error": "OpenAI API Key not configured."})
        
    combined_emails_text = "Here are the emails to analyze:\n\n"
    for i, email_text in enumerate(emails_data):
        combined_emails_text += f"---\nEmail {i+1}:\n{email_text}\n"
    combined_emails_text += "\n---"

    try:
        client = openai.AsyncOpenAI(api_key=api_key)
        
        system_prompt = "You are a helpful assistant analyzing a batch of employee emails. Answer the user's specific query based only on the provided emails."
        
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"User Query: {custom_prompt}\n\n{combined_emails_text}"}
            ],
            temperature=0.2
        )
        
        answer = response.choices[0].message.content
        return JSONResponse(content={"status": "success", "answer": answer})
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

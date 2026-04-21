from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import requests
import os
import io
import pandas as pd
from datetime import datetime

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def email_count_page(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/email_count.html", {"request": request, "user": user})

@router.post("/export")
async def export_emails(
    request: Request,
    employee_email: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...)
):
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise HTTPException(status_code=500, detail="Microsoft 365 OAuth credentials not configured in environment.")

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
        raise HTTPException(status_code=500, detail=f"Failed to authenticate with Microsoft Graph: {str(e)}")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    start_iso = f"{start_date}T00:00:00Z"
    end_iso = f"{end_date}T23:59:59Z"
    
    # Graph API endpoint with initial URL
    url = f"https://graph.microsoft.com/v1.0/users/{employee_email}/mailFolders/sentitems/messages"
    filter_query = f"sentDateTime ge {start_iso} and sentDateTime le {end_iso}"
    
    params = {
        "$filter": filter_query,
        "$select": "id,subject,sentDateTime,toRecipients",
        "$top": "999"
    }
    
    all_messages = []
    
    try:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        data = res.json()
        all_messages.extend(data.get("value", []))
        
        while "@odata.nextLink" in data:
            next_url = data["@odata.nextLink"]
            res = requests.get(next_url, headers=headers)
            res.raise_for_status()
            data = res.json()
            all_messages.extend(data.get("value", []))
            
    except Exception as e:
        error_info = getattr(e, 'response', None)
        err_msg = error_info.text if error_info else str(e)
        if "Request_ResourceNotFound" in err_msg:
            raise HTTPException(status_code=404, detail=f"Mailbox not found for {employee_email} or missing permissions.")
        raise HTTPException(status_code=500, detail=f"Failed to query emails: {err_msg}")

    # Prepare DataFrame
    results = []
    for m in all_messages:
        to_list = [r.get("emailAddress", {}).get("address", "") for r in m.get("toRecipients", [])]
        to_str = ", ".join([addr for addr in to_list if addr])
        
        results.append({
            "Subject": m.get("subject"),
            "Sent DateTime": m.get("sentDateTime"),
            "To": to_str
        })
        
    df = pd.DataFrame(results)
    
    # Generate Excel in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sent Emails')
    output.seek(0)
    
    headers_resp = {
        'Content-Disposition': f'attachment; filename="Email_Count_{employee_email}_{start_date}_to_{end_date}.xlsx"'
    }
    
    return StreamingResponse(output, headers=headers_resp, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

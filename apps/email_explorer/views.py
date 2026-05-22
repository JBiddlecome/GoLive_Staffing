from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import requests
import os
import io

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def email_explorer_page(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/email_explorer.html", {"request": request, "user": user})

def _get_access_token():
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise HTTPException(status_code=500, detail="Microsoft 365 OAuth credentials not configured.")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }

    r = requests.post(token_url, data=token_data)
    r.raise_for_status()
    return r.json().get("access_token")

def _get_all_tenant_mailboxes(access_token):
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    url = "https://graph.microsoft.com/v1.0/users"
    params = {"$select": "mail,userPrincipalName", "$top": "999", "$filter": "accountEnabled eq true"}
    mailboxes = []
    while url:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        data = res.json()
        for u in data.get("value", []):
            addr = u.get("mail") or u.get("userPrincipalName", "")
            if addr and "@" in addr and not addr.startswith("#"):
                mailboxes.append(addr.strip())
        url = data.get("@odata.nextLink")
        params = None  # nextLink already includes params
    return mailboxes

def _fetch_all_matching_emails(
    access_token, mailboxes, start_date, end_date,
    sender=None, recipient=None, keyword=None,
    include_body=False, max_pages_per_mailbox=5
):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Prefer": 'outlook.body-content-type="text"'
    }

    start_iso = f"{start_date}T00:00:00Z"
    end_iso = f"{end_date}T23:59:59Z"
    filter_query = f"receivedDateTime ge {start_iso} and receivedDateTime le {end_iso}"

    select_fields = "id,subject,bodyPreview,receivedDateTime,sentDateTime,sender,toRecipients"
    if include_body:
        select_fields += ",body,uniqueBody"

    initial_params = {
        "$filter": filter_query,
        "$select": select_fields,
        "$top": "999",
        "$orderby": "receivedDateTime desc"
    }

    # Normalize filter values
    sender_filters = [s.strip().lower() for s in sender.split(",") if s.strip()] if sender else []
    recipient_lower = recipient.lower().strip() if recipient else None
    keyword_lower = keyword.lower().strip() if keyword else None

    all_messages = []
    mailbox_list = [m.strip() for m in mailboxes.split(",") if m.strip()]

    for mailbox in mailbox_list:
        url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages"
        page_count = 0

        while url and page_count < max_pages_per_mailbox:
            try:
                if page_count == 0:
                    res = requests.get(url, headers=headers, params=initial_params)
                else:
                    res = requests.get(url, headers=headers)  # nextLink already includes params
                res.raise_for_status()
                data = res.json()
                messages = data.get("value", [])

                for m in messages:
                    m["_mailbox"] = mailbox

                    if sender_filters:
                        msg_sender = m.get("sender", {}).get("emailAddress", {}).get("address", "").lower()
                        if not any(sf in msg_sender for sf in sender_filters):
                            continue

                    if recipient_lower:
                        recipients = m.get("toRecipients", [])
                        if not any(recipient_lower in r.get("emailAddress", {}).get("address", "").lower() for r in recipients):
                            continue

                    if keyword_lower:
                        subject = (m.get("subject") or "").lower()
                        preview = (m.get("bodyPreview") or "").lower()
                        if keyword_lower not in subject and keyword_lower not in preview:
                            continue

                    all_messages.append(m)

                url = data.get("@odata.nextLink")
                page_count += 1
            except Exception as e:
                print(f"Error fetching for mailbox {mailbox}: {str(e)}")
                break

    all_messages.sort(key=lambda x: x.get("receivedDateTime", x.get("sentDateTime", "")), reverse=True)
    return all_messages

def _format_messages_for_ui(messages, include_body=False):
    results = []
    for m in messages:
        sender_addr = m.get("sender", {}).get("emailAddress", {}).get("address", "")
        to_list = [r.get("emailAddress", {}).get("address", "") for r in m.get("toRecipients", [])]
        to_str = ", ".join([addr for addr in to_list if addr])

        entry = {
            "id": m.get("id"),
            "mailbox": m.get("_mailbox"),
            "subject": m.get("subject"),
            "date": m.get("receivedDateTime", m.get("sentDateTime")),
            "sender": sender_addr,
            "to": to_str,
            "preview": m.get("bodyPreview", ""),
        }

        if include_body:
            unique_body = m.get("uniqueBody", {}).get("content", "")
            body_content = m.get("body", {}).get("content", "")
            entry["body"] = unique_body if unique_body else body_content

        results.append(entry)
    return results

def _resolve_mailboxes(access_token, mailboxes_input, search_all):
    if search_all:
        return _get_all_tenant_mailboxes(access_token)
    return mailboxes_input or ""

@router.post("/fetch")
async def fetch_emails(
    request: Request,
    mailboxes: str = Form(""),
    start_date: str = Form(...),
    end_date: str = Form(...),
    sender: str = Form(None),
    recipient: str = Form(None),
    keyword: str = Form(None),
    search_all: str = Form(None)
):
    try:
        access_token = _get_access_token()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    is_all = search_all == "true"
    if not is_all and not mailboxes.strip():
        return JSONResponse(status_code=400, content={"error": "Please enter at least one mailbox address or enable 'Search all tenant mailboxes'."})

    try:
        if is_all:
            mailbox_list = _get_all_tenant_mailboxes(access_token)
            mailboxes_str = ",".join(mailbox_list)
        else:
            mailboxes_str = mailboxes

        messages = _fetch_all_matching_emails(
            access_token, mailboxes_str, start_date, end_date,
            sender=sender, recipient=recipient, keyword=keyword,
            include_body=False, max_pages_per_mailbox=5
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error fetching emails: {str(e)}"})

    total_count = len(messages)
    formatted = _format_messages_for_ui(messages[:100], include_body=False)

    return JSONResponse(content={
        "status": "success",
        "data": formatted,
        "total_count": total_count,
        "displayed_count": len(formatted)
    })

@router.post("/download")
async def download_emails(
    request: Request,
    mailboxes: str = Form(""),
    start_date: str = Form(...),
    end_date: str = Form(...),
    sender: str = Form(None),
    recipient: str = Form(None),
    keyword: str = Form(None),
    search_all: str = Form(None)
):
    try:
        access_token = _get_access_token()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    is_all = search_all == "true"
    if not is_all and not mailboxes.strip():
        return JSONResponse(status_code=400, content={"error": "No mailboxes specified."})

    try:
        if is_all:
            mailbox_list = _get_all_tenant_mailboxes(access_token)
            mailboxes_str = ",".join(mailbox_list)
        else:
            mailboxes_str = mailboxes

        messages = _fetch_all_matching_emails(
            access_token, mailboxes_str, start_date, end_date,
            sender=sender, recipient=recipient, keyword=keyword,
            include_body=True, max_pages_per_mailbox=15
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error fetching emails: {str(e)}"})

    formatted = _format_messages_for_ui(messages, include_body=True)

    output = io.StringIO()
    output.write("Email Explorer Export\n")
    output.write(f"Mailboxes Searched: {'All tenant mailboxes' if is_all else mailboxes}\n")
    output.write(f"Date Range: {start_date} to {end_date}\n")
    if sender:
        output.write(f"From Filter: {sender}\n")
    if recipient:
        output.write(f"To Filter: {recipient}\n")
    if keyword:
        output.write(f"Keyword Filter: {keyword}\n")
    output.write(f"Total Results: {len(formatted)}\n")
    output.write("=" * 60 + "\n\n")

    for msg in formatted:
        output.write(f"Date: {msg['date']}\n")
        output.write(f"Mailbox: {msg['mailbox']}\n")
        output.write(f"From: {msg['sender']}\n")
        output.write(f"To: {msg['to']}\n")
        output.write(f"Subject: {msg['subject']}\n")
        output.write("-" * 60 + "\n")
        body_text = msg.get("body") or "[No Body Content]"
        output.write(f"{body_text}\n")
        output.write("=" * 60 + "\n\n")

    output.seek(0)
    filename = f"email_export_{start_date}_to_{end_date}.txt"
    resp_headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(iter([output.getvalue()]), media_type="text/plain", headers=resp_headers)

import asyncio
import json
import os
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import openai

from apps.orders.knowledge_base import detect_client_from_text, build_client_kb, get_staffing_manager_for_client

def _resolve_data_dir() -> Path:
    import os
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/var/data")
    return Path("data")

_STATE_FILE = _resolve_data_dir() / "orders_inbox.json"
PT = ZoneInfo("America/Los_Angeles")
TARGET_MAILBOXES = ["jake@golivestaffing.com", "michael@culinarystaffing.com", "marlen@culinarystaffing.com"]

# The schema for the AI classifier
CLASSIFICATION_PROMPT = """You are an email analyzer for a staffing agency.
Read the email subject and body. Determine if this email contains a NEW staffing order/request for staff, or if it is an UPDATE to an existing order (such as adding a shift, removing a shift, or updating/changing shifts).

CRITICAL THREAD RULE:
Emails often contain a full conversation history (thread chain) below the latest message. You must evaluate whether the LATEST (top-most) message in the email chain is introducing/requesting a new order or an update.
- If the latest message is a simple confirmation, thank you, approval, or follow-up that does not request new shifts or changes (e.g., "Thanks!", "Looks good!", "Confirmed", "Thank you, see you tomorrow", "We got it", "All set"), you MUST set both "is_order" and "is_update" to false. Do not classify the email as an order or update just because historical replies in the chain contain staffing requests.
- If the latest message explicitly references or forwards the historical message below to request booking (e.g., "Please book the shifts below", "Can we repeat this order?", "Please see the forwarded request"), then and only then should you evaluate the historical details and set "is_order" or "is_update" to true.

Return ONLY valid JSON matching this schema:
{
    "is_order": boolean,
    "is_update": boolean,
    "confidence": float (0.0 to 1.0)
}
If it is neither a new order nor an update/change to an order, set both to false.
"""

def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            with open(_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"processed_ids": [], "pending_tickets": []}

def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        tmp.replace(_STATE_FILE)
    except Exception as e:
        print(f"[Orders Email Monitor] Error saving state: {e}")

def _get_access_token() -> str | None:
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print("[Orders Email Monitor] Missing O365 credentials – skipping.")
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    try:
        r = requests.post(token_url, data=data, timeout=15)
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        print(f"[Orders Email Monitor] Token fetch failed: {e}")
        return None

def _fetch_recent_messages(token: str, mailbox: str) -> list[dict]:
    cutoff_utc = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox}/mailFolders/inbox/messages"
        f"?$filter=receivedDateTime ge {cutoff_utc}"
        f"&$select=id,subject,from,receivedDateTime,bodyPreview,body"
        f"&$top=50"
        f"&$orderby=receivedDateTime desc"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": 'outlook.body-content-type="text"'
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("value", [])
    except Exception as e:
        print(f"[Orders Email Monitor] Failed to fetch messages: {e}")
        return []

async def classify_email(subject: str, body: str) -> dict:
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY") or os.getenv("STAND_ALONE"))
        user_msg = f"Subject: {subject}\n\nBody:\n{body[:2000]}"
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": CLASSIFICATION_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)
    except Exception as e:
        print(f"[Orders Email Monitor] AI Classification Error: {e}")
        return {"is_order": False, "is_update": False, "confidence": 0}

async def email_monitoring_loop():
    await asyncio.sleep(10) # Wait for startup
    if os.getenv("RENDER", "").lower() != "true":
        print("[Orders Email Monitor] Local environment detected. Running monitor locally.")

    print("[Orders Email Monitor] Started.")

    while True:
        try:
            state = _load_state()
            processed_ids = state.get("processed_ids", [])
            pending_tickets = state.get("pending_tickets", [])
            
            token = _get_access_token()
            if token:
                for mailbox in TARGET_MAILBOXES:
                    messages = _fetch_recent_messages(token, mailbox)
                    for msg in messages:
                        msg_id = msg.get("id")
                        if not msg_id or msg_id in processed_ids:
                            continue
                            
                        sender_email = msg.get("from", {}).get("emailAddress", {}).get("address", "").lower()
                        sender_name = msg.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
                        subject = msg.get("subject", "(no subject)")
                        body_preview = msg.get("bodyPreview", "")
                        body_content = msg.get("body", {}).get("content", "")
                        received = msg.get("receivedDateTime", "")
                        
                        ignored_emails = {"michael@culinarystaffing.com", "marlen@culinarystaffing.com", "jake@golivestaffing.com"}
                        if sender_email in ignored_emails:
                            processed_ids.append(msg_id)
                            continue
                        
                        # See if this sender matches an active client
                        client_id = detect_client_from_text(sender_email)
                        if client_id:
                            # Call AI to classify if it's an order
                            classification = await classify_email(subject, body_content)
                            if classification.get("is_order") or classification.get("is_update"):
                                # Get client info to save name
                                kb = build_client_kb(client_id)
                                client_name = kb.get("name", "Unknown Client")
                                staffing_manager = get_staffing_manager_for_client(client_id)
                                
                                new_ticket = {
                                    "id": msg_id,
                                    "account": mailbox,
                                    "client_id": client_id,
                                    "client_name": client_name,
                                    "sender_name": sender_name,
                                    "sender_email": sender_email,
                                    "subject": subject,
                                    "preview": body_preview,
                                    "body": body_content,
                                    "received": received,
                                    "is_update": classification.get("is_update", False),
                                    "staffing_manager": staffing_manager
                                }
                                pending_tickets.append(new_ticket)
                                print(f"[Orders Email Monitor] Added new ticket for {client_name} from {sender_email} (Account: {mailbox})")
                        
                        # Always mark as processed so we don't hit the DB/AI for it again
                        processed_ids.append(msg_id)
            
            # Trim processed_ids to keep file small (last 1000)
            if len(processed_ids) > 1000:
                processed_ids = processed_ids[-1000:]
                
            state["processed_ids"] = processed_ids
            state["pending_tickets"] = pending_tickets
            _save_state(state)
            
        except Exception as e:
            print(f"[Orders Email Monitor] Error in loop: {e}")
            
        await asyncio.sleep(300) # Poll every 5 minutes

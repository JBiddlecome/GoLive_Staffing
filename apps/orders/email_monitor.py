import asyncio
import json
import os
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import openai

from apps.orders.knowledge_base import detect_client_from_text, build_client_kb

_STATE_FILE = Path("data/orders_inbox.json")
PT = ZoneInfo("America/Los_Angeles")
TARGET_MAILBOX = "jake@golivestaffing.com"

# The schema for the AI classifier
CLASSIFICATION_PROMPT = """You are an email analyzer for a staffing agency.
Read the email subject and body. Determine if this email contains a NEW staffing order/request for staff, or if it is an UPDATE to an existing order.
Return ONLY valid JSON matching this schema:
{
    "is_order": boolean,
    "is_update": boolean,
    "confidence": float (0.0 to 1.0)
}
If it is neither an order nor an update, set both to false.
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

def _fetch_recent_messages(token: str) -> list[dict]:
    cutoff_utc = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/mailFolders/inbox/messages"
        f"?$filter=receivedDateTime ge {cutoff_utc}"
        f"&$select=id,subject,from,receivedDateTime,bodyPreview,body"
        f"&$top=50"
        f"&$orderby=receivedDateTime desc"
    )
    headers = {"Authorization": f"Bearer {token}"}
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
        print("[Orders Email Monitor] Local environment detected. Skipping loop to avoid duplicates.")
        return

    print("[Orders Email Monitor] Started.")

    while True:
        try:
            state = _load_state()
            processed_ids = state.get("processed_ids", [])
            pending_tickets = state.get("pending_tickets", [])
            
            token = _get_access_token()
            if token:
                messages = _fetch_recent_messages(token)
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
                    
                    # See if this sender matches an active client
                    client_id = detect_client_from_text(sender_email)
                    if client_id:
                        # Call AI to classify if it's an order
                        classification = await classify_email(subject, body_content)
                        if classification.get("is_order") or classification.get("is_update"):
                            # Get client info to save name
                            kb = build_client_kb(client_id)
                            client_name = kb.get("name", "Unknown Client")
                            
                            new_ticket = {
                                "id": msg_id,
                                "client_id": client_id,
                                "client_name": client_name,
                                "sender_name": sender_name,
                                "sender_email": sender_email,
                                "subject": subject,
                                "preview": body_preview,
                                "body": body_content,
                                "received": received,
                                "is_update": classification.get("is_update", False)
                            }
                            pending_tickets.append(new_ticket)
                            print(f"[Orders Email Monitor] Added new ticket for {client_name} from {sender_email}")
                    
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

"""
Email Forwarder Scheduler
=========================
Polls michael@culinarystaffing.com and marlen@culinarystaffing.com every
2 minutes via Microsoft Graph API.

Forwarding is active when ALL of the following are true:
  1. The manual toggle is ON  (set via the Staffing Tools UI)
  2. The current Pacific time (PDT/PST) is within the forwarding schedule:
       Mon–Fri  00:00 – 09:00 AND 17:30 – 23:59 PT
       Sat–Sun  00:00 – 23:59  (all day)

NOTE: Microsoft Graph API returns receivedDateTime in UTC (Zulu). The
cutoff passed to the API is always computed and formatted in UTC so that
the filter correctly matches email received timestamps.

Emails are forwarded to staffingteam@culinarystaffing.com.
The original messages are LEFT in place (not deleted / moved).
A dedup file prevents re-forwarding the same message on the next poll.
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_STATE_FILE = Path("apps/email_forwarder/forwarder_state.json")

# ---------------------------------------------------------------------------
# In-memory toggle (controlled by the views API)
# ---------------------------------------------------------------------------
_forwarder_enabled: bool = True   # default ON; persisted across restarts via state file


def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            with open(_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"enabled": True, "forwarded_ids": [], "log": []}


def _save_state(state: dict) -> None:
    try:
        tmp = _STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        tmp.replace(_STATE_FILE)
    except Exception as e:
        print(f"[Email Forwarder] Error saving state: {e}")


def get_enabled() -> bool:
    state = _load_state()
    return state.get("enabled", True)


def set_enabled(value: bool) -> None:
    state = _load_state()
    state["enabled"] = value
    _save_state(state)
    print(f"[Email Forwarder] Toggle set to {'ON' if value else 'OFF'}")


def get_log() -> list:
    state = _load_state()
    return state.get("log", [])


# ---------------------------------------------------------------------------
# Schedule check
# ---------------------------------------------------------------------------
PT = ZoneInfo("America/Los_Angeles")  # Handles both PDT (UTC-7) and PST (UTC-8) automatically

SCHEDULE = {
    # weekday() -> list of (start_hour_min, end_hour_min) or None for all-day
    0: [((0, 0), (9, 0)), ((17, 30), (23, 59))],   # Monday
    1: [((0, 0), (9, 0)), ((17, 30), (23, 59))],   # Tuesday
    2: [((0, 0), (9, 0)), ((17, 30), (23, 59))],   # Wednesday
    3: [((0, 0), (9, 0)), ((17, 30), (23, 59))],   # Thursday
    4: [((0, 0), (9, 0)), ((17, 30), (23, 59))],   # Friday
    5: None,                                        # Saturday – all day
    6: None,                                        # Sunday – all day
}


def _is_within_schedule() -> bool:
    now = datetime.now(PT)
    day_rules = SCHEDULE.get(now.weekday())
    if day_rules is None:
        return True  # all day

    now_hm = (now.hour, now.minute)
    for start, end in day_rules:
        if start <= now_hm <= end:
            return True
    return False


# ---------------------------------------------------------------------------
# Graph API helpers
# ---------------------------------------------------------------------------
SOURCE_MAILBOXES = [
    "michael@culinarystaffing.com",
    "marlen@culinarystaffing.com",
]
DESTINATION = "staffingteam@culinarystaffing.com"
SENDER_FOR_GRAPH = "golive@culinarystaffing.com"   # shared mailbox with Mail.Send

# Emails from these addresses will never be forwarded (case-insensitive)
IGNORED_SENDERS = {
    "mail@culinarystaffing.com",
}


def _get_access_token() -> str | None:
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print("[Email Forwarder] Missing O365 credentials – skipping.")
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
        print(f"[Email Forwarder] Token fetch failed: {e}")
        return None


def _fetch_recent_messages(token: str, mailbox: str) -> list[dict]:
    """Return up to 25 most recent inbox messages received in the last 2 hours.

    IMPORTANT: Microsoft Graph receivedDateTime is always in UTC (Zulu).  We
    must build the cutoff in UTC and emit it with a Z suffix so the OData
    filter matches correctly – using a Pacific-local time here would shift the
    window by 7–8 hours and cause misses or false positives.
    """
    from datetime import timezone, timedelta
    # Compute cutoff in UTC, format with Z suffix as required by Graph API
    cutoff_utc = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox}/mailFolders/inbox/messages"
        f"?$filter=receivedDateTime ge {cutoff_utc}"
        f"&$select=id,subject,from,toRecipients,ccRecipients,receivedDateTime,body,bodyPreview"
        f"&$top=25"
        f"&$orderby=receivedDateTime desc"
    )
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("value", [])
    except Exception as e:
        print(f"[Email Forwarder] Failed to fetch messages for {mailbox}: {e}")
        return []


def _forward_message(token: str, original: dict, source_mailbox: str) -> bool:
    """
    Forward the message to DESTINATION using sendMail.
    Sends as golive@ (has Mail.Send), clearly labelled with original sender/mailbox.
    The original stays untouched in the source inbox.
    """
    orig_from = original.get("from", {}).get("emailAddress", {})
    orig_from_name = orig_from.get("name", "Unknown")
    orig_from_addr = orig_from.get("address", "unknown")
    orig_subject = original.get("subject", "(no subject)")
    orig_body = original.get("body", {}).get("content", "")
    orig_body_type = original.get("body", {}).get("contentType", "text")
    orig_received = original.get("receivedDateTime", "")

    if orig_body_type.lower() == "html":
        fwd_body = f"""
<html><body>
<div style="padding:10px 0 14px 0; border-bottom:2px solid #e5e7eb; margin-bottom:16px; font-family:Arial,sans-serif;">
  <p style="margin:0; font-size:13px; color:#6b7280;">
    <strong style="color:#374151;">Forwarded from:</strong> {source_mailbox}<br>
    <strong style="color:#374151;">Original sender:</strong> {orig_from_name} &lt;{orig_from_addr}&gt;<br>
    <strong style="color:#374151;">Received:</strong> {orig_received}<br>
    <strong style="color:#374151;">Subject:</strong> {orig_subject}
  </p>
</div>
{orig_body}
</body></html>
"""
        body_type = "HTML"
    else:
        fwd_body = (
            f"--- Forwarded from: {source_mailbox} ---\n"
            f"Original sender: {orig_from_name} <{orig_from_addr}>\n"
            f"Received: {orig_received}\n"
            f"Subject: {orig_subject}\n\n"
            f"{orig_body}"
        )
        body_type = "Text"

    payload = {
        "message": {
            "subject": f"FWD [{source_mailbox.split('@')[0]}]: {orig_subject}",
            "body": {"contentType": body_type, "content": fwd_body},
            "toRecipients": [{"emailAddress": {"address": DESTINATION}}],
        },
        "saveToSentItems": "false",
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_FOR_GRAPH}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.post(send_url, headers=headers, json=payload, timeout=20)
        r.raise_for_status()
        return True
    except Exception as e:
        err_detail = ""
        if hasattr(e, "response") and e.response is not None:
            err_detail = e.response.text
        print(f"[Email Forwarder] Failed to forward message: {e} {err_detail}")
        return False


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------
async def email_forwarder_loop():
    """Background loop – polls every 2 minutes."""
    await asyncio.sleep(60)  # brief startup stagger

    if os.getenv("RENDER", "").lower() != "true":
        print(
            "[Email Forwarder] Local environment detected (RENDER=true missing). "
            "Background monitor disabled to prevent duplicate emails."
        )
        return

    print("[Email Forwarder] Monitor started.")

    while True:
        try:
            state = _load_state()
            forwarded_ids: list = state.get("forwarded_ids", [])
            log: list = state.get("log", [])
            enabled = state.get("enabled", True)

            if not enabled:
                print("[Email Forwarder] Toggle is OFF – skipping poll.")
            elif not _is_within_schedule():
                now = datetime.now(PT)
                print(f"[Email Forwarder] Outside schedule ({now.strftime('%A %H:%M')} PT / {now.strftime('%Z')}) – skipping poll.")
            else:
                token = _get_access_token()
                if token:
                    for mailbox in SOURCE_MAILBOXES:
                        messages = _fetch_recent_messages(token, mailbox)
                        print(f"[Email Forwarder] {mailbox}: {len(messages)} message(s) in window.")

                        for msg in messages:
                            msg_id = msg.get("id")
                            if not msg_id or msg_id in forwarded_ids:
                                continue

                            sender_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "").lower()
                            if sender_addr in IGNORED_SENDERS:
                                print(f"[Email Forwarder] Skipping message from ignored sender '{sender_addr}': {msg.get('subject', '(no subject)')}")
                                forwarded_ids.append(msg_id)  # mark as seen so it isn't re-evaluated
                                continue

                            success = _forward_message(token, msg, mailbox)
                            if success:
                                forwarded_ids.append(msg_id)
                                log_entry = {
                                    "timestamp": datetime.now(PT).strftime("%Y-%m-%d %H:%M:%S"),
                                    "source": mailbox,
                                    "destination": DESTINATION,
                                    "subject": msg.get("subject", "(no subject)"),
                                    "from": msg.get("from", {}).get("emailAddress", {}).get("address", ""),
                                    "status": "forwarded",
                                }
                                log.append(log_entry)
                                print(
                                    f"[Email Forwarder] Forwarded '{msg.get('subject')}' "
                                    f"from {mailbox} -> {DESTINATION}"
                                )

                # Trim dedup list (keep last 500)
                if len(forwarded_ids) > 500:
                    forwarded_ids = forwarded_ids[-500:]

                # Trim log (keep last 200 entries)
                if len(log) > 200:
                    log = log[-200:]

                state["forwarded_ids"] = forwarded_ids
                state["log"] = log
                _save_state(state)

        except Exception as e:
            print(f"[Email Forwarder] Exception in loop: {e}")

        await asyncio.sleep(120)  # poll every 2 minutes

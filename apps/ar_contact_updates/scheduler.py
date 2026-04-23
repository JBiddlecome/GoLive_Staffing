import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from sqlalchemy import text
from apps.ar_contact_updates.views import _engine

# In-memory storage
_ar_contacts_cache = {}  # {client_contact_id: {client_name, contact_name, contact_title, email}}
_ar_contacts_changes = []  # List of change events
_is_initialized = False
_last_email_sent_date = None

def send_weekly_ar_email(changes: list[dict]):
    sender_email = "golive@culinarystaffing.com"
    receiver_emails = ["caleb@culinarystaffing.com", "jake@culinarystaffing.com"]

    import requests

    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print("[AR Contacts] Skipping email: Microsoft 365 OAuth credentials not fully set.")
        return

    # 1. Acquire OAuth Access Token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }

    try:
        r = requests.post(token_url, data=token_data)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"[AR Contacts] Failed to authenticate with Microsoft Graph: {e}")
        return

    subject = f"AR Contacts Update: {len(changes)} Change{'s' if len(changes) != 1 else ''} Last Week"

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #047857;">AR Contacts Update — Weekly Report</h2>
        <p>The following {len(changes)} Accounts Receivable contact change(s) occurred in the past week:</p>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 800px;">
            <tr style="background-color: #f3f4f6; text-align: left;">
                <th>Timestamp</th>
                <th>Action</th>
                <th>Client Name</th>
                <th>Contact Name</th>
                <th>Title</th>
                <th>Email</th>
            </tr>
    """

    for c in changes:
        color = "#065f46" if c["action"] == "Added" else "#991b1b"
        action_html = f"<strong style='color: {color}'>{c['action']}</strong>"
        html_body += f"""
            <tr>
                <td>{c.get('timestamp', 'N/A')}</td>
                <td>{action_html}</td>
                <td>{c.get('client_name') or 'N/A'}</td>
                <td>{c.get('contact_name') or 'N/A'}</td>
                <td>{c.get('contact_title') or 'N/A'}</td>
                <td>{c.get('email') or 'N/A'}</td>
            </tr>
        """

    html_body += """
        </table>
        <p style="margin-top: 20px; font-size: 12px; color: #6b7280;"><em>This is an automated alert from GoLive Staffing Tools.</em></p>
      </body>
    </html>
    """

    email_msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": email}} for email in receiver_emails],
        },
        "saveToSentItems": "false",
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        print(f"[AR Contacts] Successfully sent weekly report email to {receiver_emails}.")
    except Exception as e:
        print(f"[AR Contacts] Failed to send email via MS Graph: {e}")


def fetch_current_ar_contacts():
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                cc.client_contact_id,
                c.name AS client_name,
                cc.name AS contact_name,
                cc.title AS contact_title,
                cc.email
            FROM client_contact cc
            JOIN client c ON cc.client_id = c.client_id
            WHERE cc.accounts_receivable = 1
        """)
        with engine.begin() as connection:
            result = connection.execute(sql).mappings().all()
            return {
                str(r["client_contact_id"]): {
                    "client_name": r["client_name"] or "N/A",
                    "contact_name": r["contact_name"] or "N/A",
                    "contact_title": r["contact_title"] or "N/A",
                    "email": r["email"] or "N/A",
                }
                for r in result
            }
    except Exception as e:
        print(f"[AR Contacts] Error fetching data: {e}")
        return None
    finally:
        engine.dispose()

def get_ar_changes_log():
    return sorted(_ar_contacts_changes, key=lambda x: x["timestamp"], reverse=True)

async def ar_contacts_monitoring_loop():
    global _ar_contacts_cache, _is_initialized, _ar_contacts_changes, _last_email_sent_date

    # Stagger startup
    await asyncio.sleep(20)

    print("[AR Contacts] Monitor started.")

    while True:
        try:
            current_contacts = fetch_current_ar_contacts()
            
            if current_contacts is not None:
                if not _is_initialized:
                    # Initial load, just populate the cache without recording changes
                    _ar_contacts_cache = current_contacts
                    _is_initialized = True
                    print(f"[AR Contacts] Initialized with {len(_ar_contacts_cache)} contacts.")
                else:
                    # Diff with cache
                    current_ids = set(current_contacts.keys())
                    cached_ids = set(_ar_contacts_cache.keys())
                    
                    added_ids = current_ids - cached_ids
                    removed_ids = cached_ids - current_ids
                    
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    for cid in added_ids:
                        contact = current_contacts[cid]
                        change = {
                            "timestamp": timestamp,
                            "action": "Added",
                            "client_name": contact["client_name"],
                            "contact_name": contact["contact_name"],
                            "contact_title": contact["contact_title"],
                            "email": contact["email"],
                        }
                        _ar_contacts_changes.append(change)
                        _ar_contacts_cache[cid] = contact
                        print(f"[AR Contacts] Added: {contact['contact_name']} at {contact['client_name']}")
                        
                    for cid in removed_ids:
                        contact = _ar_contacts_cache[cid]
                        change = {
                            "timestamp": timestamp,
                            "action": "Removed",
                            "client_name": contact["client_name"],
                            "contact_name": contact["contact_name"],
                            "contact_title": contact["contact_title"],
                            "email": contact["email"],
                        }
                        _ar_contacts_changes.append(change)
                        del _ar_contacts_cache[cid]
                        print(f"[AR Contacts] Removed: {contact['contact_name']} at {contact['client_name']}")
                        
                    # Limit the changes log to keep memory usage bounded
                    if len(_ar_contacts_changes) > 1000:
                        _ar_contacts_changes = _ar_contacts_changes[-1000:]
                        
        except Exception as e:
            print(f"[AR Contacts] Exception in loop: {e}")

        # Check for weekly email
        try:
            now_la = datetime.now(ZoneInfo("America/Los_Angeles"))
            # Monday is weekday() == 0
            if now_la.weekday() == 0 and now_la.hour >= 9:
                today_str = now_la.strftime("%Y-%m-%d")
                if _last_email_sent_date != today_str:
                    seven_days_ago = now_la - timedelta(days=7)
                    recent_changes = []
                    for c in _ar_contacts_changes:
                        try:
                            # If no timezone is recorded in timestamp, assume local time LA
                            ctime = datetime.strptime(c["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/Los_Angeles"))
                            if ctime >= seven_days_ago:
                                recent_changes.append(c)
                        except:
                            pass
                    
                    if recent_changes:
                        send_weekly_ar_email(recent_changes)
                    else:
                        print("[AR Contacts] No changes this week, skipping email.")
                    
                    _last_email_sent_date = today_str
        except Exception as e:
            print(f"[AR Contacts] Error sending weekly email: {e}")

        await asyncio.sleep(900)  # 15 minutes

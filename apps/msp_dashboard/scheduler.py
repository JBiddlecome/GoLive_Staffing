import asyncio
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from sqlalchemy import text
from zoneinfo import ZoneInfo
from apps.msp_dashboard.views import _engine

_sent_shift_records = set()

def fetch_latest_15m_confirmations():
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                se.employee_id,
                ev.event_id,
                e.first_name, 
                e.last_name, 
                IFNULL(NULLIF(ev.title, ''), v.name) AS event_name, 
                m.name AS msp_name, 
                s.start AS shift_date
            FROM shift_employee se
            JOIN employee e ON se.employee_id = e.employee_id AND e.deleted_at IS NULL
            JOIN event ev ON se.event_id = ev.event_id AND ev.deleted_at IS NULL
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            JOIN client c ON ev.client_id = c.client_id AND c.deleted_at IS NULL
            JOIN msp m ON c.msp_id = m.id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            WHERE 
                se.confirmed = 1 
                AND c.msp_id IN (3, 5, 20)
                AND se.deleted_at IS NULL
                AND se.confirmed_at >= :fifteen_mins_ago
            ORDER BY se.confirmed_at DESC;
        """)
        
        la_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        fifteen_mins_ago = (la_time - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        
        with engine.begin() as connection:
            result = connection.execute(sql, {"fifteen_mins_ago": fifteen_mins_ago}).mappings().all()
            return [dict(r) for r in result]
    except Exception as e:
        print(f"Error fetching scheduler data: {e}")
        return []
    finally:
        engine.dispose()

def send_msp_alert_email(confirmations: list[dict]):
    sender_email = "golive@culinarystaffing.com"
    receiver_emails = ["jake@culinarystaffing.com", "suraj@culinarystaffing.com", "sankalp@culinarystaffing.com"]
    
    import requests
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        print("Skipping email: Microsoft 365 OAuth credentials (O365_TENANT_ID, O365_CLIENT_ID, O365_CLIENT_SECRET) not fully set.")
        return

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
        print(f"Failed to authenticate with Microsoft Graph: {e}")
        if error_info: print(error_info.text)
        return

    subject = f"MSP Dashboard Alert: {len(confirmations)} New Confirmations"
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #065f46;">MSP New Confirmations Alert</h2>
        <p>The following {len(confirmations)} employees have been confirmed for MSP clients in the last 15 minutes:</p>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 800px;">
            <tr style="background-color: #f3f4f6; text-align: left;">
                <th>Employee Name</th>
                <th>Event</th>
                <th>MSP</th>
                <th>Shift Date</th>
            </tr>
    """
    
    for c in confirmations:
        employee_name = f"{c.get('first_name')} {c.get('last_name')}"
        shift_date = c.get('shift_date').strftime("%Y-%m-%d %H:%M") if c.get('shift_date') else 'N/A'
        
        html_body += f"""
            <tr>
                <td>{employee_name}</td>
                <td>{c.get('event_name') or 'N/A'}</td>
                <td>{c.get('msp_name') or 'N/A'}</td>
                <td>{shift_date}</td>
            </tr>
        """
        
    html_body += """
        </table>
        <p style="margin-top: 20px; font-size: 12px; color: #6b7280;"><em>This is an automated alert from GoLive Staffing Tools.</em></p>
      </body>
    </html>
    """
    
    # 2. Build MS Graph Email Payload
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [
                {"emailAddress": {"address": email}} for email in receiver_emails
            ]
        },
        "saveToSentItems": "false"
    }

    # 3. Dispatch Email
    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        print(f"Successfully sent MSP alert email for {len(confirmations)} confirmations via MS Graph.")
    except Exception as e:
        error_info = getattr(e, 'response', None)
        print(f"Failed to send email via MS Graph: {e}")
        if error_info: print(error_info.text)

async def msp_monitoring_loop():
    global _sent_shift_records
    
    # Random offset to avoid running right as server starts heavily
    await asyncio.sleep(30) 
    
    while True:
        try:
            data = fetch_latest_15m_confirmations()
            
            new_confirmations = []
            for d in data:
                record_id = f"{d.get('employee_id')}-{d.get('event_id')}"
                if record_id not in _sent_shift_records:
                    new_confirmations.append(d)
            
            if len(new_confirmations) >= 1:
                send_msp_alert_email(new_confirmations)
                
                for c in new_confirmations:
                    _sent_shift_records.add(f"{c.get('employee_id')}-{c.get('event_id')}")
                    
                if len(_sent_shift_records) > 500:
                    _sent_shift_records = set(list(_sent_shift_records)[-250:])
                    
        except Exception as e:
            print(f"[MSP Monitor] Exception in loop: {e}")
            
        await asyncio.sleep(900)  # 15 minutes

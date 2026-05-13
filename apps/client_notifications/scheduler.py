import asyncio
import base64
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import pandas as pd
from sqlalchemy import text
from apps.client_notifications.utils import _engine, _get_setting, get_prior_month_data

LA_TZ = ZoneInfo("America/Los_Angeles")

async def send_client_monthly_notification(client_id: int, activity: dict, recipients: list):
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id_o365 = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id_o365, client_secret]):
        print("[Client Notifications] Credentials missing; aborting email dispatch.")
        return False

    # Get OAuth Token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id_o365,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    try:
        r = requests.post(token_url, data=token_data, timeout=15)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"[Client Notifications] Authentication failed: {e}")
        return False

    # Generate HTML
    positions_html = "".join([
        f"<tr><td>{p['position']}</td><td>{f'{p['hours']:.1f}' if p.get('hours') is not None else '0.0'}</td><td>{p.get('employee_count', 0)}</td></tr>"
        for p in activity['positions']
    ])

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #4f46e5;">Monthly Activity Review - {activity['client_name']}</h2>
        <p>Hello,</p>
        <p>Here is a summary of your activity with GoLive Staffing for the prior month:</p>
        
        <table border="0" cellpadding="10" cellspacing="0" style="width: 100%; max-width: 600px; background-color: #f8fafc; border-radius: 12px; margin-bottom: 20px;">
          <tr>
            <td><strong>Preferred Employees Added:</strong></td>
            <td>{activity['preferred_added_count']}</td>
          </tr>
          <tr>
            <td><strong>Total Hours Worked:</strong></td>
            <td>{f"{activity['total_hours']:.1f}" if activity['total_hours'] is not None else '0.0'}</td>
          </tr>
          <tr>
            <td><strong>Average Employee Rating:</strong></td>
            <td>{f"{activity['avg_rating']:.1f}" if activity['avg_rating'] else 'N/A'}</td>
          </tr>
        </table>

        <h3>Positions Worked</h3>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 600px;">
          <tr style="background-color: #f1f5f9;">
            <th>Position</th>
            <th>Hours</th>
            <th>Employees</th>
          </tr>
          {positions_html}
        </table>

        <h3>Staffing Performance</h3>
        <ul>
          <li><strong>Average time before creating a shift:</strong> {f"{activity['avg_creation_to_start_hours']:.1f}" if activity['avg_creation_to_start_hours'] else 'N/A'} hours</li>
          <li><strong>Average time to fill a shift:</strong> {f"{activity['avg_creation_to_fill_hours']:.1f}" if activity['avg_creation_to_fill_hours'] else 'N/A'} hours</li>
          <li><strong>Fill rate (>24h notice):</strong> {f"{activity['fill_rate_over_24h']:.0f}" if activity['fill_rate_over_24h'] is not None else 'N/A'}{'%' if activity['fill_rate_over_24h'] is not None else ''}</li>
          <li><strong>Fill rate (<24h notice):</strong> {f"{activity['fill_rate_under_24h']:.0f}" if activity['fill_rate_under_24h'] is not None else 'N/A'}{'%' if activity['fill_rate_under_24h'] is not None else ''}</li>
        </ul>

        <p style="margin-top: 25px; font-size: 12px; color: #64748b;">
          This is an automated report from GoLive Staffing.
        </p>
      </body>
    </html>
    """

    msg_payload = {
        "message": {
            "subject": f"Monthly Activity Review: {activity['client_name']}",
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [{"emailAddress": {"address": email}} for email in recipients],
        },
        "saveToSentItems": "false"
    }

    send_url = "https://graph.microsoft.com/v1.0/users/golive@culinarystaffing.com/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(send_url, headers=headers, json=msg_payload, timeout=30)
        response.raise_for_status()
        return True
    except Exception as exc:
        print(f"[Client Notifications] Failed to send to {recipients}: {exc}")
        return False

async def client_notifications_loop():
    await asyncio.sleep(300) # Wait 5 mins after startup

    if os.getenv("RENDER", "").lower() != "true":
        print("[Client Notifications] Local environment; disabling background loop.")
        return

    print("[Client Notifications] Monitor ACTIVE.")
    
    while True:
        try:
            now = datetime.now(LA_TZ)
            # Run on the first Tuesday of the month at 10:00 AM
            if now.weekday() == 1 and 1 <= now.day <= 7 and now.hour == 10:
                
                # Check if already sent this month
                month_tag = now.strftime("%Y-%m")
                last_sent = _get_setting("last_sent_month_year")
                if last_sent == month_tag:
                    await asyncio.sleep(3600)
                    continue

                if not _get_setting("global_send_enabled", False):
                    print("[Client Notifications] Sending is disabled via global toggle.")
                    await asyncio.sleep(3600)
                    continue

                print(f"[Client Notifications] Starting monthly dispatch for {month_tag}")
                
                engine = _engine()
                # 1. Get clients who had orders last month
                first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                last_month_end = first_of_this_month - timedelta(days=1)
                last_month_start = last_month_end.replace(day=1)
                
                with engine.begin() as conn:
                    clients = conn.execute(text("""
                        SELECT DISTINCT c.client_id, c.name, u.email as sm_email
                        FROM event e
                        JOIN client c ON e.client_id = c.client_id
                        LEFT JOIN venue v ON e.venue_id = v.venue_id
                        LEFT JOIN user u ON v.staffing_manager_id = u.id
                        WHERE e.date BETWEEN :start AND :end 
                          AND e.deleted_at IS NULL
                    """), {
                        "start": last_month_start.strftime("%Y-%m-%d"),
                        "end": last_month_end.strftime("%Y-%m-%d")
                    }).fetchall()
                
                processed_count = 0
                for cid, cname, sm_email in clients:
                    activity = await get_prior_month_data(cid)
                    activity['client_name'] = cname
                    
                    # Determine recipients
                    recipients = []
                    if _get_setting("test_email_only", False):
                        recipients = ["test@culinarystaffing.com"]
                    elif _get_setting("staffing_manager_only", False):
                        if sm_email: recipients = [sm_email]
                    else:
                        # Get all client contacts
                        with engine.begin() as conn:
                            contacts = conn.execute(text("SELECT email FROM client_contact WHERE client_id = :id AND email IS NOT NULL AND email != ''"), {"id": cid}).fetchall()
                            recipients = [r[0] for r in contacts]
                            # Fallback to Staffing Manager if no contacts? User didn't specify.
                            if not recipients and sm_email: recipients = [sm_email]

                    if recipients:
                        success = await send_client_monthly_notification(cid, activity, recipients)
                        if success: processed_count += 1
                        # Throttle slightly
                        await asyncio.sleep(2)
                    
                    # If test_email_only is on, maybe we only process one or a few?
                    if _get_setting("test_email_only", False) and processed_count >= 5:
                        break

                from apps.client_notifications.utils import _set_setting
                _set_setting("last_sent_month_year", month_tag)
                print(f"[Client Notifications] Completed monthly dispatch. Processed {processed_count} clients.")

        except Exception as e:
            print(f"[Client Notifications] Loop error: {e}")

        await asyncio.sleep(1800) # Check every 30 mins

import asyncio
import base64
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text

from apps.reports.views import _get_basic_client_summary_df, _get_basic_client_detail_df

_STATE_FILE = Path("apps/reports/google_report_state.json")
LA_TZ = ZoneInfo("America/Los_Angeles")

def _load_sent_state() -> dict:
    if _STATE_FILE.exists():
        try:
            with open(_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_sent_month_year": ""}

def _save_sent_state(month_year: str) -> None:
    # Ensure only persistent environments log tracking, prevents clutter locally
    if os.getenv("RENDER", "").lower() != "true":
        return
    try:
        with open(_STATE_FILE, "w") as f:
            json.dump({"last_sent_month_year": month_year}, f)
    except Exception as e:
        print(f"[Google Report Scheduler] Failed to save state: {e}")

def is_first_wednesday_at_noon(now_dt: datetime) -> bool:
    # Day 2 is Wednesday (0=Mon, 1=Tue, 2=Wed)
    is_wednesday = now_dt.weekday() == 2
    # Between the 1st and 7th means it's the first occurrence
    is_first_of_month = 1 <= now_dt.day <= 7
    # Hour 12 (Noon)
    is_noon = now_dt.hour == 12
    return is_wednesday and is_first_of_month and is_noon

async def send_google_spruce_goose_report() -> bool:
    """Retrieves previous month data, generates Excel, and emails via Graph API."""
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        print("[Google Report Scheduler] Credentials missing; aborting email dispatch.")
        return False

    # Get OAuth Token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    try:
        r = requests.post(token_url, data=token_data, timeout=15)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"[Google Report Scheduler] Authentication failed: {e}")
        return False

    now = datetime.now(LA_TZ)
    
    # Calculate date range for the previous full month
    # First step: jump back to the first day of current month
    first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Step two: go back one day to land on the last day of the previous month
    last_day_of_prev_month = first_of_this_month - timedelta(days=1)
    # Step three: jump to the first day of the target month
    first_day_of_prev_month = last_day_of_prev_month.replace(day=1)
    
    start_date = first_day_of_prev_month.strftime("%Y-%m-%d")
    end_date = last_day_of_prev_month.strftime("%Y-%m-%d")
    client_id_value = 1226 # Google Spruce Goose Offices
    
    print(f"[Google Report Scheduler] Running export for dates {start_date} to {end_date}")
    
    try:
        summary_df = await _get_basic_client_summary_df(client_id_value, start_date, end_date, by_venue=True)
        detail_df = await _get_basic_client_detail_df(client_id_value, start_date, end_date)
        
        if summary_df.empty:
            print("[Google Report Scheduler] Warning: Report has no data. Sending empty file structure anyway.")
            
        # Transform headers for nicer looking Excel files, mirroring core report logic
        summary_cols = {
            "employee_name": "Employee Name",
            "positions_worked": "Positions Worked",
            "total_hours": "Total Hours",
            "list_status": "List Status",
            "venue_name": "Venue Name",
        }
        summary_export = summary_df.rename(columns=summary_cols)
        if "Venue Name" in summary_export.columns:
            cols = ["Venue Name"] + [c for c in summary_export.columns if c != "Venue Name"]
            summary_export = summary_export[cols]

        detail_export = detail_df.rename(columns={
            "shift_date": "Date of Shift",
            "venue_name": "Venue Name",
            "employee_name": "Employee Name",
            "position": "Position",
            "scheduled_hours": "Scheduled Hours",
            "worked_hours": "Worked Hours",
            "pay_rate": "Pay Rate",
            "bill_rate": "Bill Rate",
            "list_status": "List Status",
        })

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            summary_export.to_excel(writer, index=False, sheet_name="Employee Summary")
            detail_export.to_excel(writer, index=False, sheet_name="Shift Details")
        output.seek(0)
        
        excel_binary = output.read()
        b64_content = base64.b64encode(excel_binary).decode("utf-8")
        
        readable_month_range = f"{first_day_of_prev_month.strftime('%B %Y')}"
        filename = f"Google_Spruce_Goose_Report_{start_date}_to_{end_date}.xlsx"
        
        sender = "golive@culinarystaffing.com"
        recipient = "dan.stone@culinarystaffing.com"
        subject = f"Monthly Basic Client Report - Google Spruce Goose Offices ({readable_month_range})"
        
        body_html = f"""
        <html>
          <body style="font-family: Arial, sans-serif; color: #333;">
            <p>Hi Dan,</p>
            <p>Attached is the monthly automated Basic Client Report for the <strong>Google Spruce Goose Offices</strong> client account.</p>
            <ul style="background: #f8fafc; padding: 15px 35px; border-radius: 8px; border: 1px solid #e2e8f0;">
              <li><strong>Account:</strong> Google Spruce Goose Offices (ID 1226)</li>
              <li><strong>Time Span:</strong> {first_day_of_prev_month.strftime('%m/%d/%Y')} – {last_day_of_prev_month.strftime('%m/%d/%Y')}</li>
              <li><strong>Configuration:</strong> Separated by Venue Location Enabled</li>
            </ul>
            <p>This is an automated report from the GoLive Staffing Tools dashboard.</p>
            <p style="margin-top: 25px;">Best regards,<br/>GoLive Tools Automation</p>
          </body>
        </html>
        """

        # Construct MS Graph sendMail payload including the fileAttachment
        msg_payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body_html
                },
                "toRecipients": [{"emailAddress": {"address": recipient}}],
                "attachments": [
                    {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "contentBytes": b64_content
                    }
                ]
            },
            "saveToSentItems": "false"
        }

        send_url = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(send_url, headers=headers, json=msg_payload, timeout=30)
        response.raise_for_status()
        
        print(f"[Google Report Scheduler] Successfully emailed report to {recipient}")
        return True
        
    except Exception as exc:
        print(f"[Google Report Scheduler] Runtime crash occurred generating/sending: {exc}")
        return False

async def google_spruce_goose_report_loop():
    """Background monitor loop check run on server startup."""
    # Stagger offset so loop wakes up shortly after app comes fully alive
    await asyncio.sleep(180)

    if os.getenv("RENDER", "").lower() != "true":
        print("[Google Report Scheduler] Outside Render environment (local). Disabling automated background execution loop.")
        return

    print("[Google Report Scheduler] Startup check complete. Monitor ACTIVE.")
    
    while True:
        try:
            current_la_time = datetime.now(LA_TZ)
            current_month_tag = current_la_time.strftime("%Y-%m")
            
            if is_first_wednesday_at_noon(current_la_time):
                state = _load_sent_state()
                
                # Ensure it has not run already during the 12:00-12:59 window
                if state.get("last_sent_month_year") != current_month_tag:
                    print(f"[Google Report Scheduler] Confirmed First Wednesday @ Noon condition. Attempting dispatch for period {current_month_tag}.")
                    success = await send_google_spruce_goose_report()
                    if success:
                        _save_sent_state(current_month_tag)
        except Exception as error:
            print(f"[Google Report Scheduler] Exception cycle interrupt: {error}")

        # Recheck once every 20 minutes. State storage mechanism blocks duplicate sends inside the 1-hour window.
        await asyncio.sleep(1200)

from __future__ import annotations

import base64
import io
import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )


def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)


def get_previous_week_range() -> tuple[str, str]:
    """Calculate the previous Monday to Sunday date range."""
    # Current date in Los Angeles timezone (which is standard for this app)
    la_tz = ZoneInfo("America/Los_Angeles")
    today = datetime.now(la_tz)
    days_since_monday = today.weekday()  # 0 is Monday, 6 is Sunday
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday.strftime("%Y-%m-%d"), last_sunday.strftime("%Y-%m-%d")


def fetch_no_shows(start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Retrieve all no-show timesheets and aggregate their notes."""
    engine = _engine()
    try:
        sql = text("""
            SELECT 
                t.timesheet_id,
                t.employee_id,
                t.event_id,
                t.shift_employee_id,
                CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
                ev.date AS event_date,
                s.start AS shift_start,
                s.end AS shift_end,
                c.name AS client_name,
                v.name AS venue_name,
                t.employee_worked,
                t.client_worked,
                se.cancel_reason,
                t.employee_notes,
                t.client_notes,
                se.cancel_notes,
                se.daily_report_notes,
                se.note_to_employee
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN employee emp ON t.employee_id = emp.employee_id
            JOIN event ev ON t.event_id = ev.event_id
            JOIN client c ON ev.client_id = c.client_id
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            WHERE (
                t.employee_worked = 'NOSHOW'
                OR t.client_worked = 'NOSHOW'
                OR se.cancel_reason = 14
            )
            AND se.deleted_at IS NULL
            AND ev.deleted_at IS NULL
            AND s.deleted_at IS NULL
            AND emp.deleted_at IS NULL
            AND c.deleted_at IS NULL
            AND DATE(s.start) >= :start_date
            AND DATE(s.start) <= :end_date
            ORDER BY s.start ASC, employee_name ASC
        """)

        with engine.begin() as connection:
            rows = connection.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
            records = [dict(row) for row in rows]

            for rec in records:
                se_id = rec["shift_employee_id"]

                # 1. Fetch employee notes linked to this shift
                emp_notes_sql = text("""
                    SELECT en.datetime, en.note, en.type, CONCAT(u.first_name, ' ', u.last_name) AS author
                    FROM employee_note en
                    LEFT JOIN user u ON en.user_id = u.id
                    WHERE en.shift_employee_id = :se_id
                    ORDER BY en.datetime DESC
                """)
                emp_notes_rows = connection.execute(emp_notes_sql, {"se_id": se_id}).mappings().all()
                emp_notes = [dict(r) for r in emp_notes_rows]

                # 2. Fetch payroll notes linked to this shift
                pay_notes_sql = text("""
                    SELECT pn.date_created AS datetime, pn.note, 'Payroll Note' AS type, CONCAT(u.first_name, ' ', u.last_name) AS author
                    FROM payroll_note pn
                    LEFT JOIN user u ON pn.user_id = u.id
                    WHERE pn.shift_employee_id = :se_id
                    ORDER BY pn.date_created DESC
                """)
                pay_notes_rows = connection.execute(pay_notes_sql, {"se_id": se_id}).mappings().all()
                pay_notes = [dict(r) for r in pay_notes_rows]

                # Combine notes into a single list
                all_notes = []
                for n in emp_notes:
                    dt_str = n["datetime"].strftime("%Y-%m-%d %H:%M") if n["datetime"] else ""
                    author = n["author"] if n["author"] else "System"
                    all_notes.append({
                        "type": f"Employee Note ({n['type'] or 'General'})",
                        "date": dt_str,
                        "author": author,
                        "note": n["note"]
                    })
                for n in pay_notes:
                    dt_str = n["datetime"].strftime("%Y-%m-%d %H:%M") if n["datetime"] else ""
                    author = n["author"] if n["author"] else "System"
                    all_notes.append({
                        "type": "Payroll Note",
                        "date": dt_str,
                        "author": author,
                        "note": n["note"]
                    })

                rec["additional_notes"] = all_notes

                # Format datetimes for JSON serialization
                if rec["event_date"]:
                    rec["event_date"] = rec["event_date"].strftime("%Y-%m-%d")
                if rec["shift_start"]:
                    rec["shift_start"] = rec["shift_start"].strftime("%Y-%m-%d %H:%M")
                if rec["shift_end"]:
                    rec["shift_end"] = rec["shift_end"].strftime("%Y-%m-%d %H:%M")

            return records
    finally:
        engine.dispose()


def generate_no_show_excel(records: list[dict[str, Any]]) -> bytes:
    """Generate Excel binary from no-show records list."""
    rows = []
    for rec in records:
        # Format the notes list nicely as a string block
        direct_notes = []
        if rec.get("employee_notes"):
            direct_notes.append(f"Employee Timesheet Notes: {rec['employee_notes']}")
        if rec.get("client_notes"):
            direct_notes.append(f"Client Timesheet Notes: {rec['client_notes']}")
        if rec.get("cancel_notes"):
            direct_notes.append(f"Placement Cancel Notes: {rec['cancel_notes']}")
        if rec.get("daily_report_notes"):
            direct_notes.append(f"Daily Report Notes: {rec['daily_report_notes']}")
        if rec.get("note_to_employee"):
            direct_notes.append(f"Note to Employee: {rec['note_to_employee']}")
            
        for n in rec.get("additional_notes", []):
            direct_notes.append(f"[{n['type']} - {n['date']} by {n['author']}]: {n['note']}")
            
        notes_str = "\n".join(direct_notes)

        rows.append({
            "Employee Name": rec.get("employee_name"),
            "Shift Date": rec.get("event_date"),
            "Shift Start": rec.get("shift_start"),
            "Shift End": rec.get("shift_end"),
            "Client Name": rec.get("client_name"),
            "Venue Name": rec.get("venue_name") or "",
            "Employee Worked Status": rec.get("employee_worked"),
            "Client Worked Status": rec.get("client_worked"),
            "Placement Cancel Reason Code": rec.get("cancel_reason"),
            "Notes Summary": notes_str
        })

    df = pd.DataFrame(rows)
    if df.empty:
        # Create empty DataFrame with columns
        df = pd.DataFrame(columns=[
            "Employee Name", "Shift Date", "Shift Start", "Shift End",
            "Client Name", "Venue Name", "Employee Worked Status",
            "Client Worked Status", "Placement Cancel Reason Code", "Notes Summary"
        ])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="No Show Report")
    output.seek(0)
    return output.read()


def send_no_show_report_email(
    start_date: str,
    end_date: str,
    recipient: str | list[str] = "jake@culinarystaffing.com"
) -> dict[str, Any]:
    """Query, compile, and email the No Show Report via MS Graph API or mock it locally."""
    if isinstance(recipient, str):
        recipients = [r.strip() for r in recipient.split(",") if r.strip()]
    else:
        recipients = list(recipient)

    records = fetch_no_shows(start_date, end_date)
    excel_bin = generate_no_show_excel(records)
    b64_content = base64.b64encode(excel_bin).decode("utf-8")

    # Generate HTML body
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2 style="color: #065f46; border-bottom: 2px solid #34d399; padding-bottom: 8px;">No Show Report</h2>
        <p>This is the automated weekly No Show Report for the period: <strong>{start_date}</strong> to <strong>{end_date}</strong>.</p>
    """

    if not records:
        html_body += f"""
        <div style="background-color: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 16px; margin: 20px 0;">
            <p style="color: #166534; font-weight: bold; margin: 0;">Excellent! No employee timesheets with a No Show status were recorded for this period.</p>
        </div>
        """
    else:
        html_body += f"""
        <p>Found <strong>{len(records)}</strong> employee timesheet(s) marked with No Show status:</p>
        <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse; width: 100%; font-size: 14px; margin-top: 15px; border-color: #e2e8f0;">
            <thead>
                <tr style="background-color: #f8fafc; text-align: left; border-bottom: 2px solid #cbd5e1;">
                    <th style="font-weight: 600; color: #334155;">Employee Name</th>
                    <th style="font-weight: 600; color: #334155;">Shift Date / Time</th>
                    <th style="font-weight: 600; color: #334155;">Client / Venue Name</th>
                    <th style="font-weight: 600; color: #334155;">Notes</th>
                </tr>
            </thead>
            <tbody>
        """
        for r in records:
            # Build shift date and time
            dt_display = r["event_date"]
            if r["shift_start"]:
                time_only = r["shift_start"].split(" ")[1]
                dt_display += f" @ {time_only}"
                
            venue_display = r["client_name"]
            if r["venue_name"]:
                venue_display += f" ({r['venue_name']})"

            # Build notes block
            notes_html = []
            if r.get("employee_notes"):
                notes_html.append(f"<strong>Emp Timesheet:</strong> {r['employee_notes']}")
            if r.get("client_notes"):
                notes_html.append(f"<strong>Client Timesheet:</strong> {r['client_notes']}")
            if r.get("cancel_notes"):
                notes_html.append(f"<strong>Cancel Notes:</strong> {r['cancel_notes']}")
            if r.get("daily_report_notes"):
                notes_html.append(f"<strong>Daily Report Notes:</strong> {r['daily_report_notes']}")
            if r.get("note_to_employee"):
                notes_html.append(f"<strong>Note to Employee:</strong> {r['note_to_employee']}")
                
            for an in r.get("additional_notes", []):
                notes_html.append(f"<strong>{an['type']}</strong> ({an['author']}): {an['note']}")

            notes_block = "<br/>".join(notes_html) if notes_html else "<em style='color:#94a3b8;'>No notes recorded</em>"

            html_body += f"""
                <tr style="border-bottom: 1px solid #e2e8f0; vertical-align: top;">
                    <td style="font-weight: bold; color: #0f172a;">{r['employee_name']}</td>
                    <td style="color: #475569;">{dt_display}</td>
                    <td style="color: #475569;">{venue_display}</td>
                    <td style="color: #334155; font-size: 13px;">{notes_block}</td>
                </tr>
            """
        html_body += """
            </tbody>
        </table>
        """

    html_body += """
        <p style="margin-top: 30px; font-size: 12px; color: #64748b; border-top: 1px solid #e2e8f0; padding-top: 10px;">
            <em>This report was generated and delivered automatically by the GoLive Staffing Tools system.</em>
        </p>
      </body>
    </html>
    """

    subject = f"Weekly No Show Report: {start_date} to {end_date}"
    filename = f"No_Show_Report_{start_date}_to_{end_date}.xlsx"
    sender_email = "golive@culinarystaffing.com"

    # 1. Check if O365 credentials exist. If not, trigger a mock send.
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        # Mocking for local testing
        print("[No Show Report] O365 credentials missing from environment. Performing local dry-run.")
        
        # Save HTML email to output folder as a mock file
        mock_output_dir = Path("scratch")
        mock_output_dir.mkdir(exist_ok=True)
        mock_file = mock_output_dir / f"mock_no_show_email_{start_date}_to_{end_date}.html"
        with open(mock_file, "w", encoding="utf-8") as f:
            f.write(html_body)
            
        return {
            "success": True,
            "mocked": True,
            "recipient": ", ".join(recipients),
            "subject": subject,
            "record_count": len(records),
            "info": f"Dry-run report compiled. Mock HTML saved to scratch/{mock_file.name}. excel attachment created successfully."
        }

    # 2. MS Graph API OAuth flow
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
        print(f"[No Show Report] Token retrieval failed: {e}")
        return {
            "success": False,
            "error": f"Microsoft OAuth failed: {str(e)}"
        }

    # MS Graph Payload
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
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

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        res = requests.post(send_url, headers=headers, json=email_msg, timeout=30)
        res.raise_for_status()
        return {
            "success": True,
            "mocked": False,
            "recipient": ", ".join(recipients),
            "subject": subject,
            "record_count": len(records),
            "info": f"Successfully emailed No Show Report with {len(records)} records to {', '.join(recipients)}."
        }
    except Exception as e:
        error_info = getattr(e, 'response', None)
        detail = error_info.text if error_info else str(e)
        print(f"[No Show Report] Email dispatch failed: {detail}")
        return {
            "success": False,
            "error": f"Failed to send email via Microsoft Graph: {detail}"
        }


@router.get("", response_class=HTMLResponse)
async def no_show_report_page(request: Request):
    """Serve the No Show Report dashboard page."""
    start_date, end_date = get_previous_week_range()
    return templates.TemplateResponse(
        "apps/no_show_report.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )


@router.get("/data")
async def get_no_shows_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """API endpoint to retrieve no-show timesheet details in JSON format."""
    try:
        data = fetch_no_shows(start_date, end_date)
        return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/send-email")
async def trigger_no_show_email(
    request: Request,
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """Endpoint allowing admins to manually trigger the report email for a custom date range."""
    # Authenticate admin if needed (handled globally by fastapi request check in app.py)
    # Target email is default jake@culinarystaffing.com
    res = send_no_show_report_email(start_date, end_date)
    if res["success"]:
        return JSONResponse(res)
    else:
        return JSONResponse(res, status_code=500)

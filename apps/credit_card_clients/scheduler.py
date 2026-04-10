import asyncio
from datetime import datetime, timedelta
import os
import json
from pathlib import Path
from sqlalchemy import text
from zoneinfo import ZoneInfo
from apps.credit_card_clients.views import _engine

_sent_emails_file = Path("apps/credit_card_clients/cc_sent_emails.json")


def _load_sent_records():
    if _sent_emails_file.exists():
        try:
            with open(_sent_emails_file, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def _save_sent_records(records_set):
    with open(_sent_emails_file, "w") as f:
        json.dump(list(records_set), f)


def fetch_latest_15m_cc_cancellations():
    """Fetch credit-card client cancellations from the last 15 minutes.
    Uses COALESCE across deleted_at, cancelled_at, and shift start to handle
    records where deleted_at may be NULL."""
    engine = _engine()
    try:
        sql = text("""
            SELECT
                se.shift_employee_id,
                se.event_id,
                c.name            AS client_name,
                p.description     AS position,
                se.bill_rate      AS bill_rate,
                s.start           AS shift_date
            FROM shift_employee se
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s           ON sp.shift_id          = s.shift_id
            JOIN position p        ON sp.position_id       = p.position_id
            JOIN event ev          ON se.event_id          = ev.event_id
            JOIN client c          ON ev.client_id         = c.client_id
            WHERE se.cancel_reason = 4
              AND c.payment_type   = 1
              AND (
                  se.deleted_at   >= :fifteen_mins_ago
                  OR se.cancelled_at >= :fifteen_mins_ago
              )
            ORDER BY COALESCE(se.deleted_at, se.cancelled_at) DESC;
        """)

        la_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        fifteen_mins_ago = (la_time - timedelta(minutes=15)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with engine.begin() as connection:
            result = connection.execute(
                sql, {"fifteen_mins_ago": fifteen_mins_ago}
            ).mappings().all()
            return [dict(r) for r in result]
    except Exception as e:
        print(f"[CC Clients] Error fetching scheduler data: {e}")
        return []
    finally:
        engine.dispose()


def send_cc_cancellation_alert_email(cancellations: list[dict]):
    """Send an email alert for new credit-card client cancellations via MS Graph."""
    sender_email = "golive@culinarystaffing.com"
    receiver_email = "jake@culinarystaffing.com"

    import requests

    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print(
            "[CC Clients] Skipping email: Microsoft 365 OAuth credentials "
            "(O365_TENANT_ID, O365_CLIENT_ID, O365_CLIENT_SECRET) not fully set."
        )
        return

    # 1. Acquire OAuth Access Token
    token_url = (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    )
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
        error_info = getattr(e, "response", None)
        print(f"[CC Clients] Failed to authenticate with Microsoft Graph: {e}")
        if error_info:
            print(error_info.text)
        return

    subject = (
        f"Credit Card Client Alert: {len(cancellations)} Cancelled "
        f"Shift{'s' if len(cancellations) != 1 else ''}"
    )

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #991b1b;">Credit Card Client — Cancelled Shifts</h2>
        <p>The following {len(cancellations)} shift(s) were cancelled by credit card clients:</p>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 800px;">
            <tr style="background-color: #f3f4f6; text-align: left;">
                <th>Client Name</th>
                <th>Position</th>
                <th>Bill Rate</th>
                <th>Shift Date</th>
            </tr>
    """

    for c in cancellations:
        bill_rate = (
            f"${float(c.get('bill_rate', 0)):.2f}"
            if c.get("bill_rate")
            else "N/A"
        )
        shift_date = (
            c.get("shift_date").strftime("%Y-%m-%d %H:%M")
            if c.get("shift_date")
            else "N/A"
        )

        html_body += f"""
            <tr>
                <td>{c.get('client_name') or 'N/A'}</td>
                <td>{c.get('position') or 'N/A'}</td>
                <td>{bill_rate}</td>
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
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [
                {"emailAddress": {"address": receiver_email}}
            ],
        },
        "saveToSentItems": "false",
    }

    # 3. Dispatch Email
    send_url = (
        f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        print(
            f"[CC Clients] Successfully sent alert email for "
            f"{len(cancellations)} cancellation(s) via MS Graph."
        )
    except Exception as e:
        error_info = getattr(e, "response", None)
        print(f"[CC Clients] Failed to send email via MS Graph: {e}")
        if error_info:
            print(error_info.text)


async def cc_clients_monitoring_loop():
    """Background loop that checks every 15 minutes for new credit-card client cancellations."""
    _sent_cancellation_records = _load_sent_records()

    # Stagger startup to avoid colliding with MSP monitor
    await asyncio.sleep(45)

    while True:
        try:
            data = fetch_latest_15m_cc_cancellations()
            print(f"[CC Clients] Poll found {len(data)} cancellation(s) in last 15m window.")

            new_cancellations = []
            for d in data:
                record_id = str(d.get("shift_employee_id"))
                if record_id not in _sent_cancellation_records:
                    new_cancellations.append(d)

            if new_cancellations:
                print(f"[CC Clients] {len(new_cancellations)} new cancellation(s) to alert on.")
                send_cc_cancellation_alert_email(new_cancellations)

                for c in new_cancellations:
                    _sent_cancellation_records.add(str(c.get("shift_employee_id")))

                _save_sent_records(_sent_cancellation_records)

                if len(_sent_cancellation_records) > 500:
                    _sent_cancellation_records = set(
                        list(_sent_cancellation_records)[-250:]
                    )
                    _save_sent_records(_sent_cancellation_records)
            else:
                print("[CC Clients] No new cancellations to alert on.")

        except Exception as e:
            print(f"[CC Clients Monitor] Exception in loop: {e}")

        await asyncio.sleep(900)  # 15 minutes

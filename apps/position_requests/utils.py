import os
import re
from sqlalchemy import text
from apps.position_requests.scheduler import _engine

def send_position_added_email(employee_email: str, first_name: str, added_positions: list):
    sender_email = "golive@culinarystaffing.com"
    
    import requests
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret, employee_email]):
        print("Skipping email: Microsoft 365 OAuth credentials missing or employee email is empty.")
        return False
        
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
        print(f"Failed to authenticate with Microsoft Graph: {e}")
        return False

    subject = "New Position(s) Added to Your GoLive! Profile"
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #047857;">Profile Update</h2>
        <p>Hi {first_name},</p>
        <p>Good news! Your request has been approved, and the following position(s) have been added to your profile:</p>
        <ul>
    """
    for p in added_positions:
        html_body += f"<li><strong>{p}</strong></li>"
        
    html_body += f"""
        </ul>
        <p>You will now be eligible to request shifts for these positions in GoLive!</p>
        <p>Best regards,<br>The Culinary Staffing Team</p>
      </body>
    </html>
    """
    
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [
                {"emailAddress": {"address": employee_email}}
            ]
        },
        "saveToSentItems": "true"
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to send email via MS Graph: {e}")
        return False


def add_position_to_employee(phone: str, positions: str, name: str = None) -> tuple[bool, str]:
    clean_phone = re.sub(r'\D', '', phone)
    if len(clean_phone) > 10:
        clean_phone = clean_phone[-10:]
        
    engine = _engine()
    try:
        with engine.connect() as conn:
            employee_id = None
            first_name = "Employee"
            employee_email = None

            if clean_phone and len(clean_phone) >= 10:
                emp_sql = text("""
                    SELECT employee_id, first_name, email 
                    FROM employee 
                    WHERE REPLACE(REPLACE(REPLACE(REPLACE(mobile, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                       OR REPLACE(REPLACE(REPLACE(REPLACE(home, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                       OR REPLACE(REPLACE(REPLACE(REPLACE(work, ' ', ''), '-', ''), '(', ''), ')', '') LIKE :phone
                    LIMIT 1
                """)
                emp_res = conn.execute(emp_sql, {"phone": f"%{clean_phone}%"}).fetchone()
                if emp_res:
                    employee_id = emp_res[0]
                    first_name = emp_res[1] or "Employee"
                    employee_email = emp_res[2]

            if not employee_id and name and name != "Unknown":
                name_sql = text("""
                    SELECT employee_id, first_name, email 
                    FROM employee 
                    WHERE CONCAT(first_name, ' ', last_name) = :name
                      AND status IN (1, 3, 6, 10, 14)
                    LIMIT 1
                """)
                name_res = conn.execute(name_sql, {"name": name}).fetchone()
                if name_res:
                    employee_id = name_res[0]
                    first_name = name_res[1] or "Employee"
                    employee_email = name_res[2]

            if not employee_id:
                return False, "Employee not found in database."

            status_sql = text("SELECT status FROM employee WHERE employee_id = :eid LIMIT 1")
            status_row = conn.execute(status_sql, {"eid": employee_id}).fetchone()
            if status_row and status_row[0] == 2:
                return False, "Denied: Employee has Candidate status and is not eligible to have positions added."
                
            # Find position_id for the given positions
            position_names = [p.strip() for p in positions.split(",") if p.strip()]
            if not position_names:
                position_names = [p.strip() for p in positions.split("\n") if p.strip()]
            if not position_names:
                 position_names = [positions.strip()]

            added_positions = []
            already_eligible_positions = []
            not_found_positions = []

            for pos_name in position_names:
                if not pos_name:
                    continue
                pos_sql = text("""
                    SELECT position_id, description FROM position
                    WHERE description LIKE :pos_name
                    LIMIT 1
                """)
                # Try exact match or starts with first
                pos_res = conn.execute(pos_sql, {"pos_name": f"{pos_name}%"}).fetchone()

                if not pos_res:
                    # Try partial match anywhere
                    pos_res = conn.execute(pos_sql, {"pos_name": f"%{pos_name}%"}).fetchone()

                if not pos_res:
                    not_found_positions.append(pos_name)
                    continue

                position_id = pos_res[0]

                # Check if employee already has this position and is already eligible
                emp_pos_sql = text("""
                    SELECT employee_position_id, eligible FROM employee_position
                    WHERE employee_id = :employee_id AND position_id = :position_id
                    LIMIT 1
                """)
                emp_pos_res = conn.execute(emp_pos_sql, {"employee_id": employee_id, "position_id": position_id}).fetchone()

                if emp_pos_res and emp_pos_res[1] == 1:
                    # Already eligible — no changes needed
                    already_eligible_positions.append(pos_res[1])
                elif emp_pos_res:
                    # Exists but not eligible — update
                    update_sql = text("""
                        UPDATE employee_position
                        SET eligible = 1, status = 1
                        WHERE employee_position_id = :employee_position_id
                    """)
                    conn.execute(update_sql, {"employee_position_id": emp_pos_res[0]})
                    conn.commit()
                    added_positions.append(pos_res[1])
                else:
                    # Insert new
                    insert_sql = text("""
                        INSERT INTO employee_position (employee_id, position_id, eligible, status, sub_type_id)
                        VALUES (:employee_id, :position_id, 1, 1, -1)
                    """)
                    conn.execute(insert_sql, {"employee_id": employee_id, "position_id": position_id})
                    conn.commit()
                    added_positions.append(pos_res[1])

            if not added_positions and not already_eligible_positions:
                return False, f"Could not match positions: {', '.join(not_found_positions)}"

            msg_parts = []
            if already_eligible_positions:
                msg_parts.append(f"Already eligible: {', '.join(already_eligible_positions)}")
            if added_positions:
                msg_parts.append(f"Newly added/updated: {', '.join(added_positions)}")
            if not_found_positions:
                msg_parts.append(f"Could not find: {', '.join(not_found_positions)}")
            msg = ". ".join(msg_parts)

            if added_positions and employee_email:
                email_sent = send_position_added_email(employee_email, first_name, added_positions)
                msg += ". Notification email sent." if email_sent else ". Failed to send email."

            return True, msg
    except Exception as e:
        return False, f"Database error: {str(e)}"
    finally:
        engine.dispose()

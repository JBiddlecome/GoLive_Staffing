from sqlalchemy import text
from apps.position_requests.scheduler import _engine
import json
import re

def detect_client_from_text(order_text: str) -> int:
    """
    Extracts emails from the text and attempts to find a matching active client.
    First tries an exact email match, then falls back to a domain match.
    Returns the client_id if found, otherwise None.
    """
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', order_text)
    if not emails:
        return None
        
    engine = _engine()
    with engine.connect() as conn:
        # First pass: Exact email match
        for email in emails:
            # We want to make sure the client is active (status = 1)
            # and not deleted. The GoLive DB has `client_contact` linked to `client`.
            sql = text("""
                SELECT c.client_id 
                FROM client_contact cc
                JOIN client c ON cc.client_id = c.client_id
                WHERE cc.email = :email 
                  AND c.status = 1 
                  AND c.deleted_at IS NULL
                LIMIT 1
            """)
            res = conn.execute(sql, {"email": email}).fetchone()
            if res:
                return res.client_id
                
        # Second pass: Domain match (skip generic domains like gmail.com)
        generic_domains = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com'}
        for email in emails:
            domain = email.split('@')[-1].lower()
            if domain in generic_domains:
                continue
                
            domain_search = f"%@{domain}"
            sql = text("""
                SELECT c.client_id 
                FROM client_contact cc
                JOIN client c ON cc.client_id = c.client_id
                WHERE cc.email LIKE :domain 
                  AND c.status = 1 
                  AND c.deleted_at IS NULL
                LIMIT 1
            """)
            res = conn.execute(sql, {"domain": domain_search}).fetchone()
            if res:
                return res.client_id
                
    return None

def get_active_clients() -> list:
    """Returns a list of all active clients (id and name)."""
    engine = _engine()
    with engine.connect() as conn:
        sql = text("""
            SELECT client_id, name 
            FROM client 
            WHERE status = 1 
              AND deleted_at IS NULL 
            ORDER BY name ASC
        """)
        rows = conn.execute(sql).fetchall()
        return [{"id": r.client_id, "name": r.name} for r in rows]

def build_client_kb(client_id: int) -> dict:
    """
    Build a dynamic Client Knowledge Base for the AI Extractor.
    This queries the database to understand what venues, positions,
    and employees the client typically uses.
    """
    try:
        engine = _engine()
        with engine.connect() as conn:
            # 1. Basic Client Info
            client_sql = text("SELECT name, industry FROM client WHERE client_id = :client_id")
            client_res = conn.execute(client_sql, {"client_id": client_id}).fetchone()
            
            if not client_res:
                return {}
                
            client_name, industry = client_res
            
            # 2. Venues
            # Get venues and their frequency of use in events
            venue_sql = text("""
                SELECT v.venue_id, v.name, v.address1, v.city, COUNT(e.event_id) as freq
                FROM venue v
                LEFT JOIN event e ON e.venue_id = v.venue_id AND e.deleted_at IS NULL
                WHERE v.client_id = :client_id 
                  AND v.deleted_at IS NULL 
                  AND v.status = 1
                GROUP BY v.venue_id, v.name, v.address1, v.city
                ORDER BY freq DESC
            """)
            venues = conn.execute(venue_sql, {"client_id": client_id}).fetchall()
            typical_venues = [
                {"venue_id": v.venue_id, "name": v.name, "address": f"{v.address1}, {v.city}", "frequency": v.freq}
                for v in venues
            ]
            
            # 3. Typical Positions (from history)
            # Find the most frequently requested positions in the last year
            pos_sql = text("""
                SELECT p.position_id, p.description, COUNT(sp.shift_position_id) as freq
                FROM event e
                JOIN shift s ON s.event_id = e.event_id
                JOIN shift_position sp ON sp.shift_id = s.shift_id
                JOIN position p ON p.position_id = sp.position_id
                WHERE e.client_id = :client_id
                  AND e.deleted_at IS NULL
                  AND sp.deleted_at IS NULL
                GROUP BY p.position_id, p.description
                ORDER BY freq DESC
                LIMIT 15
            """)
            positions = conn.execute(pos_sql, {"client_id": client_id}).fetchall()
            available_positions = [
                {"position_id": p.position_id, "name": p.description, "frequency": p.freq}
                for p in positions
            ]
            
            # 4. Preferred Employees
            # Existence in exclusive table means preferred
            emp_sql = text("""
                SELECT e.employee_id, CONCAT(e.first_name, ' ', e.last_name) as name
                FROM exclusive ex
                JOIN employee e ON e.employee_id = ex.employee_id
                WHERE ex.client_id = :client_id AND e.status = 1
            """)
            employees = conn.execute(emp_sql, {"client_id": client_id}).fetchall()
            preferred_employees = [
                {"employee_id": e.employee_id, "name": e.name, "type": "Preferred"}
                for e in employees
            ]
            
            # Build the KB dictionary
            return {
                "client_id": client_id,
                "name": client_name,
                "industry": industry,
                "typical_venues": typical_venues,
                "available_positions": available_positions,
                "preferred_employees": preferred_employees,
                "instructions": "Use exact position names from 'available_positions'. If ambiguous, use the highest frequency position that matches. If the input explicitly states a venue name, extract it. If the input DOES NOT explicitly state a venue name, default to the venue with the highest frequency in 'typical_venues'."
            }
            
    except Exception as e:
        print(f"Error building client KB for ID {client_id}: {e}")
        return {}

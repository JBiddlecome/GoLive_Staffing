from sqlalchemy import text
from apps.position_requests.scheduler import _engine
import json
import re

def detect_client_from_text(order_text: str) -> int:
    """
    Attempts to find a matching client for the given text.
    1. Extracts emails from the text and tries exact email then domain matches.
    2. Scans the text for active client names (fuzzy word match).
    3. Scans the text for active venue names (fuzzy word match) and connects to their client.
    Returns the client_id if found, otherwise None.
    """
    if not order_text:
        return None
        
    engine = _engine()
    
    # 1. Email-based detection
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', order_text)
    if emails:
        with engine.connect() as conn:
            # First pass: Exact email match
            for email in emails:
                sql = text("""
                    SELECT c.client_id 
                    FROM client_contact cc
                    JOIN client c ON cc.client_id = c.client_id
                    WHERE cc.email = :email 
                      AND c.status IN (1, 10, 11) 
                      AND c.deleted_at IS NULL
                    LIMIT 1
                """)
                res = conn.execute(sql, {"email": email}).fetchone()
                if res:
                    return res.client_id
                    
            # Second pass: Domain match (skip generic and internal domains)
            generic_domains = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'culinarystaffing.com', 'golivestaffing.com'}
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
                      AND c.status IN (1, 10, 11) 
                      AND c.deleted_at IS NULL
                    LIMIT 1
                """)
                res = conn.execute(sql, {"domain": domain_search}).fetchone()
                if res:
                    return res.client_id

    # 2. Client Name-based detection
    try:
        with engine.connect() as conn:
            sql = text("""
                SELECT client_id, name 
                FROM client 
                WHERE status IN (1, 10, 11) 
                  AND deleted_at IS NULL
            """)
            clients = conn.execute(sql).fetchall()
            
            # Sort clients by name length descending to match more specific names first
            sorted_clients = sorted(clients, key=lambda c: len(c.name), reverse=True)
            for cid, name in sorted_clients:
                if not name:
                    continue
                # Use word boundaries and case-insensitive matching
                pattern = r'\b' + re.escape(name.strip()) + r'\b'
                if re.search(pattern, order_text, re.IGNORECASE):
                    return cid
    except Exception as e:
        print(f"Error matching client name: {e}")

    # 3. Venue Name-based detection
    try:
        with engine.connect() as conn:
            sql = text("""
                SELECT v.client_id, v.name 
                FROM venue v
                JOIN client c ON v.client_id = c.client_id
                WHERE c.status IN (1, 10, 11)
                  AND c.deleted_at IS NULL
                  AND v.status = 1
                  AND v.deleted_at IS NULL
            """)
            venues = conn.execute(sql).fetchall()
            
            # Sort venues by name length descending to match more specific names first
            sorted_venues = sorted(venues, key=lambda v: len(v.name), reverse=True)
            for cid, name in sorted_venues:
                if not name:
                    continue
                pattern = r'\b' + re.escape(name.strip()) + r'\b'
                if re.search(pattern, order_text, re.IGNORECASE):
                    return cid
    except Exception as e:
        print(f"Error matching venue name: {e}")
        
    return None

def get_active_clients() -> list:
    """Returns a list of all active or inactive 60/180 clients (id and name)."""
    engine = _engine()
    with engine.connect() as conn:
        sql = text("""
            SELECT client_id, name 
            FROM client 
            WHERE status IN (1, 10, 11) 
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
            
            # 5. Typical Shift Times
            time_sql = text("""
                SELECT p.description as position, 
                       TIME_FORMAT(s.start, '%H:%i') as start_time, 
                       TIME_FORMAT(s.end, '%H:%i') as end_time,
                       COUNT(*) as freq
                FROM event e
                JOIN shift s ON s.event_id = e.event_id
                JOIN shift_position sp ON sp.shift_id = s.shift_id
                JOIN position p ON p.position_id = sp.position_id
                WHERE e.client_id = :client_id
                  AND e.deleted_at IS NULL
                  AND s.start IS NOT NULL 
                  AND s.end IS NOT NULL
                GROUP BY p.description, start_time, end_time
                ORDER BY freq DESC
                LIMIT 50
            """)
            times = conn.execute(time_sql, {"client_id": client_id}).fetchall()
            typical_times = {}
            for t in times:
                if t.position not in typical_times:
                    typical_times[t.position] = []
                if len(typical_times[t.position]) < 3: # Keep top 3 times per position
                    typical_times[t.position].append({
                        "start_time": t.start_time,
                        "end_time": t.end_time,
                        "frequency": t.freq
                    })
            
            # Build the KB dictionary
            return {
                "client_id": client_id,
                "name": client_name,
                "industry": industry,
                "typical_venues": typical_venues,
                "available_positions": available_positions,
                "preferred_employees": preferred_employees,
                "typical_shift_times": typical_times,
                "instructions": "Use exact position names from 'available_positions'. If ambiguous, use the highest frequency position that matches. If the input explicitly states a venue name, extract it. If the input DOES NOT explicitly state a venue name, default to the venue with the highest frequency in 'typical_venues'. If the email only specifies a start time, use 'typical_shift_times' for that position to infer the end time. If an employee is requested by name, match them against 'preferred_employees' and extract their ID."
            }
            
    except Exception as e:
        print(f"Error building client KB for ID {client_id}: {e}")
        return {}

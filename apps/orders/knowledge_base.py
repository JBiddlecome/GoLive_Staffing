from sqlalchemy import text
from apps.position_requests.scheduler import _engine
import json

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
            venue_sql = text("""
                SELECT venue_id, name, address1, city 
                FROM venue 
                WHERE client_id = :client_id 
                  AND deleted_at IS NULL 
                  AND status = 1
            """)
            venues = conn.execute(venue_sql, {"client_id": client_id}).fetchall()
            typical_venues = [
                {"venue_id": v.venue_id, "name": v.name, "address": f"{v.address1}, {v.city}"}
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
            # type 1 = Preferred, type 2 = Priority (from exclusive table)
            emp_sql = text("""
                SELECT ex.type, e.employee_id, CONCAT(e.first_name, ' ', e.last_name) as name
                FROM exclusive ex
                JOIN employee e ON e.employee_id = ex.employee_id
                WHERE ex.client_id = :client_id AND e.status = 1
            """)
            employees = conn.execute(emp_sql, {"client_id": client_id}).fetchall()
            preferred_employees = [
                {"employee_id": e.employee_id, "name": e.name, "type": "Preferred" if e.type == 1 else "Priority"}
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
                "instructions": "Use exact position names from 'available_positions'. If ambiguous, use the highest frequency position that matches."
            }
            
    except Exception as e:
        print(f"Error building client KB for ID {client_id}: {e}")
        return {}

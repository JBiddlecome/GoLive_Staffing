from sqlalchemy import text
from apps.position_requests.scheduler import _engine
import json
import re

def clean_and_stem_words(name_str: str) -> list[str]:
    """
    Splits string into lowercase words, filters out generic terms/suffixes,
    and stems words by removing trailing 's' (for words > 3 chars).
    """
    if not name_str:
        return []
    words = re.findall(r'\b\w+\b', name_str.lower())
    generic_terms = {
        'medical', 'center', 'centers', 'catering', 'dining', 'services', 'service', 
        'llc', 'inc', 'ltd', 'club', 'hotel', 'resort', 'group', 'corporation', 'corp', 
        'co', 'company', 'university', 'college', 'hospital', 'productions', 'production', 
        'association', 'school', 'facility', 'training', 'practice', 'llp', 'incorporated',
        'of', 'and', 'the', 'by', 'dba', 'private'
    }
    core_words = [w for w in words if w not in generic_terms]
    if not core_words:
        core_words = [w for w in words if w not in {'of', 'and', 'the', 'by'}]
        
    stemmed = []
    for w in core_words:
        if len(w) > 3 and w.endswith('s') and not w.endswith('ss'):
            stemmed.append(w[:-1])
        else:
            stemmed.append(w)
    return stemmed

def is_subsequence_fuzzy(sub: list[str], main: list[str]) -> bool:
    """
    Returns True if all words in `sub` match words in `main` in the exact same relative order.
    Supports prefixes and high similarity matching.
    """
    if not sub:
        return False
    import difflib
    sub_idx = 0
    for w_main in main:
        w_sub = sub[sub_idx]
        match = False
        if w_sub == w_main:
            match = True
        elif len(w_sub) > 2 and len(w_main) > 2 and (w_sub.startswith(w_main) or w_main.startswith(w_sub)):
            match = True
        elif difflib.SequenceMatcher(None, w_sub, w_main).ratio() > 0.8:
            match = True
            
        if match:
            sub_idx += 1
            if sub_idx == len(sub):
                return True
    return False

def detect_client_from_text(order_text: str, is_dictation: bool = False) -> int:
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
    if not is_dictation:
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

            # Second pass: robust word-subsequence fuzzy matching
            order_words = clean_and_stem_words(order_text)
            if order_words:
                for cid, name in sorted_clients:
                    if not name:
                        continue
                    client_words = clean_and_stem_words(name)
                    if client_words and is_subsequence_fuzzy(client_words, order_words):
                        return cid
    except Exception as e:
        print(f"Error matching client name: {e}")

    # 3. Venue Name-based detection (runs for both email and dictation)
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

def get_staffing_manager_for_client(client_id: int) -> str | None:
    """Queries the database to find the staffing manager email associated with this client's active venues."""
    engine = _engine()
    sql = text("""
        SELECT DISTINCT u.email
        FROM venue v
        JOIN user u ON v.staffing_manager_id = u.id
        WHERE v.client_id = :client_id
          AND v.status = 1
          AND v.deleted_at IS NULL
          AND u.email IS NOT NULL
        LIMIT 1
    """)
    try:
        with engine.connect() as conn:
            res = conn.execute(sql, {"client_id": client_id}).fetchone()
            if res:
                return res[0]
            
            # Fallback to client's staff_id account manager
            fallback_sql = text("""
                SELECT u.email
                FROM client c
                JOIN user u ON c.staff_id = u.id
                WHERE c.client_id = :client_id
                  AND u.email IS NOT NULL
            """)
            res = conn.execute(fallback_sql, {"client_id": client_id}).fetchone()
            if res:
                return res[0]
    except Exception as e:
        print(f"Error fetching staffing manager for client {client_id}: {e}")
    return None

_staffing_managers_cache = {}  # {client_id: (email, cache_time)}

def get_staffing_managers_for_clients(client_ids: list[int]) -> tuple[dict[int, str], bool]:
    """Queries the database to find the staffing manager email associated with the active venues of multiple clients.
    Falls back to the client's staff_id account manager if no active venue staffing manager is found.
    Returns (resolved_dict, success_bool).
    Uses a 5-minute TTL cache to avoid hitting the database on every call.
    """
    if not client_ids:
        return {}, True
        
    import time
    now = time.time()
    ttl = 300  # 5 minutes
    
    resolved = {}
    missing_ids = []
    
    for cid in client_ids:
        if cid in _staffing_managers_cache:
            email, cache_time = _staffing_managers_cache[cid]
            if now - cache_time < ttl:
                if email:  # only add to resolved if it's not None
                    resolved[cid] = email
                continue
        missing_ids.append(cid)
        
    if not missing_ids:
        return resolved, True
        
    engine = _engine()
    db_resolved = {}
    
    # 1. Query active venues
    sql = text("""
        SELECT v.client_id, u.email
        FROM venue v
        JOIN user u ON v.staffing_manager_id = u.id
        WHERE v.client_id IN :client_ids
          AND v.status = 1
          AND v.deleted_at IS NULL
          AND u.email IS NOT NULL
    """)
    
    try:
        with engine.connect() as conn:
            res = conn.execute(sql, {"client_ids": tuple(missing_ids)}).fetchall()
            for row in res:
                cid, email = row
                if cid not in db_resolved:
                    db_resolved[cid] = email
                    
            # 2. Find fallback for clients not resolved yet
            still_missing = [cid for cid in missing_ids if cid not in db_resolved]
            if still_missing:
                fallback_sql = text("""
                    SELECT c.client_id, u.email
                    FROM client c
                    JOIN user u ON c.staff_id = u.id
                    WHERE c.client_id IN :missing_ids
                      AND u.email IS NOT NULL
                """)
                fallback_res = conn.execute(fallback_sql, {"missing_ids": tuple(still_missing)}).fetchall()
                for row in fallback_res:
                    cid, email = row
                    if cid not in db_resolved:
                        db_resolved[cid] = email
                        
            # Update cache and resolved dict
            for cid in missing_ids:
                email = db_resolved.get(cid)
                _staffing_managers_cache[cid] = (email, now)
                if email:
                    resolved[cid] = email
                    
            return resolved, True
    except Exception as e:
        print(f"Error fetching staffing managers for clients {client_ids}: {e}")
        return resolved, False



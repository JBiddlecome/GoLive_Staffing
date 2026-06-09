from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from .extractor import ai_extract_order
from .knowledge_base import build_client_kb, detect_client_from_text, get_active_clients
from .db_operations import create_order
from .email_monitor import _load_state, _save_state
from .utils import clean_email_text
from sqlalchemy import text
from apps.position_requests.scheduler import _engine

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Fallback dummy client knowledge base if DB connection fails locally
CLIENT_KB = {
    63: {
        "name": "ACME Catering",
        "available_positions": [
            {"name": "Server", "typical_grooming": "Standard Black Bistro", "tools": ["Wine Key", "Crumber"]},
            {"name": "Bartender", "typical_grooming": "Standard Black Bistro", "tools": ["Wine Key", "Shaker"]},
            {"name": "Cook 2", "typical_grooming": "Chef Coat", "tools": ["Knives"]},
            {"name": "Cook G", "typical_grooming": "Chef Coat", "tools": ["Knives", "Thermometer"]}
        ],
        "typical_positions_used": ["Server", "Cook G"],
        "typical_venues": [
            {"name": "Convention Center", "address": "123 Fake St"},
            {"name": "Downtown Hotel", "address": "456 Main St"}
        ],
        "certifications": ["Food Handler"]
    }
}

@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/orders/index.html", {"request": request, "user": user})

def normalize_extracted_positions(extracted_data: dict):
    shifts = extracted_data.get('shift_information', [])
    if not shifts:
        return
        
    engine = _engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT description FROM position")).fetchall()
            db_positions = [r[0] for r in rows]
            
            for s in shifts:
                pos = s.get('position')
                if not pos:
                    continue
                matched_pos = None
                for db_pos in db_positions:
                    if db_pos.lower().strip() == pos.lower().strip():
                        matched_pos = db_pos
                        break
                if matched_pos:
                    s['position'] = matched_pos
    except Exception as e:
        print(f"Error normalizing positions: {e}")

def resolve_existing_shifts_for_extraction(extracted_data: dict, client_id: int):
    basic = extracted_data.get('basic_information', {})
    venue_name = basic.get('venue_name')
    shifts = extracted_data.get('shift_information', [])
    
    if not venue_name or not shifts:
        return
        
    engine = _engine()
    try:
        with engine.connect() as conn:
            v_sql = text("SELECT venue_id FROM venue WHERE client_id = :client_id AND name = :v_name LIMIT 1")
            venue_row = conn.execute(v_sql, {"client_id": client_id, "v_name": venue_name}).fetchone()
            if not venue_row:
                return
            venue_id = venue_row[0]
            
            for s in shifts:
                action = s.get('action', 'CREATE')
                if action not in ('UPDATE', 'REMOVE'):
                    continue
                    
                event_date = s.get('date')
                pos_name = s.get('position')
                if not event_date or not pos_name:
                    continue
                    
                ev_sql = text("""
                    SELECT event_id FROM event 
                    WHERE client_id = :client_id AND venue_id = :venue_id AND date = :date AND deleted_at IS NULL 
                    LIMIT 1
                """)
                event_row = conn.execute(ev_sql, {"client_id": client_id, "venue_id": venue_id, "date": event_date}).fetchone()
                if not event_row:
                    continue
                event_id = event_row[0]
                
                active_shifts_sql = text("""
                    SELECT 
                        s.shift_id, s.start, s.end, 
                        sp.shift_position_id, p.description as position_name, sp.count
                    FROM shift s
                    JOIN shift_position sp ON sp.shift_id = s.shift_id
                    JOIN position p ON sp.position_id = p.position_id
                    WHERE s.event_id = :event_id
                      AND s.deleted_at IS NULL
                      AND sp.deleted_at IS NULL
                """)
                db_shifts = conn.execute(active_shifts_sql, {"event_id": event_id}).fetchall()
                
                matched_shift = None
                best_score = -1
                inc_start_time = s.get('start_time')
                
                for db_sh in db_shifts:
                    if db_sh.position_name.lower().strip() == pos_name.lower().strip():
                        score = 10
                        if inc_start_time and db_sh.start:
                            db_start_str = db_sh.start.strftime("%H:%M") if hasattr(db_sh.start, 'strftime') else str(db_sh.start).split(' ')[-1][:5]
                            if db_start_str == inc_start_time:
                                score += 20
                            else:
                                try:
                                    db_h, db_m = map(int, db_start_str.split(':'))
                                    inc_h, inc_m = map(int, inc_start_time.split(':'))
                                    diff = abs((db_h * 60 + db_m) - (inc_h * 60 + inc_m))
                                    score += max(0, 15 - (diff / 10))
                                except Exception:
                                    pass
                        if score > best_score:
                            best_score = score
                            matched_shift = db_sh
                            
                if matched_shift:
                    db_start = matched_shift.start
                    db_end = matched_shift.end
                    
                    db_start_str = db_start.strftime("%H:%M") if hasattr(db_start, 'strftime') else str(db_start).split(' ')[-1][:5]
                    db_end_str = db_end.strftime("%H:%M") if hasattr(db_end, 'strftime') else str(db_end).split(' ')[-1][:5]
                    
                    s['db_start_time'] = db_start_str
                    s['db_end_time'] = db_end_str
                    s['db_position'] = matched_shift.position_name
                    
                    # Normalise to actual DB casing and space format
                    s['position'] = matched_shift.position_name
                    
                    if not s.get('start_time') or action == 'REMOVE':
                        s['start_time'] = db_start_str
                    if not s.get('end_time') or action == 'REMOVE':
                        s['end_time'] = db_end_str
                    if s.get('end_time_inferred') or not inc_start_time:
                        s['end_time'] = db_end_str
                        s['end_time_inferred'] = False
    except Exception as e:
        print(f"Error resolving existing shifts: {e}")

@router.post("/clean")
async def clean_order_email(request: Request):
    data = await request.json()
    email_text = data.get('text', '')
    cleaned = clean_email_text(email_text)
    return JSONResponse({"status": "success", "cleaned_text": cleaned})

@router.post("/extract")
async def extract_order(request: Request):
    data = await request.json()
    text = data.get('text', '')
    client_id = data.get('client_id')
    reference_date = data.get('reference_date')
    thread_mode = data.get('thread_mode', 'latest')
    
    # If client_id is not explicitly provided, try to detect from text
    is_dictation = data.get('is_dictation', False)
    if not client_id:
        client_id = detect_client_from_text(text, is_dictation=is_dictation)
        
    if not client_id:
        active_clients = get_active_clients()
        return JSONResponse({
            "status": "error", 
            "message": "Could not auto-detect client from email. Please select a client.",
            "requires_client_selection": True,
            "clients": active_clients
        }, status_code=400)
    
    # Build knowledge base for the detected/selected client
    client_context = build_client_kb(client_id)
    if not client_context:
        return JSONResponse({"status": "error", "message": "Failed to build knowledge base for client."}, status_code=500)
    
    extracted_data = await ai_extract_order(text, client_context, reference_date=reference_date, thread_mode=thread_mode)
    
    if not extracted_data.get('basic_information'):
        return JSONResponse({"status": "error", "message": "Failed to extract order data."}, status_code=500)
        
    normalize_extracted_positions(extracted_data)
    resolve_existing_shifts_for_extraction(extracted_data, client_id)
    
    return JSONResponse({"status": "success", "data": extracted_data, "client_kb": client_context})

@router.post("/publish")
async def publish_order(request: Request):
    user = request.session.get("user")
    if not user:
        return JSONResponse({"status": "error", "message": "Unauthorized."}, status_code=401)
        
    data = await request.json()
    # Execute the insertion logic
    result = create_order(data, user.get('id', 1))
    
    if result.get("status") == "error":
        return JSONResponse(result, status_code=400)
        
    return JSONResponse(result)

@router.get("/employees/search")
def search_employees(request: Request, q: str = '', date: str = ''):
    # Searches for active (1) or inactive60 (10) employees matching query
    # Also checks if they are working on the specified date
    engine = _engine()
    
    # We match first_name or last_name or CONCAT(first_name, ' ', last_name)
    search_term = f"%{q.replace(' ', '%')}%"
    
    sql = text("""
        SELECT e.employee_id, u.first_name, u.last_name,
        (
            SELECT COUNT(*)
            FROM shift_employee se
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            JOIN event ev ON s.event_id = ev.event_id
            WHERE se.employee_id = e.employee_id 
              AND ev.date = :date 
              AND se.deleted_at IS NULL
              AND se.cancel_reason = 0
              AND se.confirmed = 1
        ) as is_working
        FROM employee e
        JOIN user_party up ON e.employee_id = up.employee_id
        JOIN user u ON up.user_id = u.id
        WHERE e.status IN (1, 10)
          AND e.deleted_at IS NULL
          AND (
            u.first_name LIKE :q OR 
            u.last_name LIKE :q OR 
            CONCAT(u.first_name, ' ', u.last_name) LIKE :q
          )
        ORDER BY u.last_name ASC, u.first_name ASC
        LIMIT 20
    """)
    
    with engine.connect() as conn:
        rows = conn.execute(sql, {"q": search_term, "date": date}).fetchall()
        
    results = []
    for row in rows:
        results.append({
            "id": row.employee_id,
            "name": f"{row.first_name} {row.last_name}",
            "is_working": row.is_working > 0
        })
        
    return JSONResponse({"status": "success", "data": results})

@router.get("/positions")
def get_positions(request: Request, client_id: int = None, venue_name: str = None):
    engine = _engine()
    positions = []
    
    if client_id and venue_name:
        # Get positions configured for this client/venue
        sql = text("""
            SELECT DISTINCT p.description 
            FROM position p
            JOIN client_position cp ON cp.position_id = p.position_id
            LEFT JOIN venue_position vp ON vp.position_id = p.position_id
            LEFT JOIN venue v ON vp.venue_id = v.venue_id AND v.name = :venue_name AND v.deleted_at IS NULL
            WHERE cp.client_id = :client_id 
              AND (vp.venue_position_id IS NOT NULL OR v.venue_id IS NULL)
            ORDER BY p.description ASC
        """)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql, {"client_id": client_id, "venue_name": venue_name}).fetchall()
                positions = [r.description for r in rows]
        except Exception as e:
            print(f"Error fetching configured positions: {e}")
            
    if not positions:
        # Fallback to typical positions from history
        if client_id:
            from .knowledge_base import build_client_kb
            kb = build_client_kb(client_id)
            if kb and kb.get("available_positions"):
                positions = [p["name"] for p in kb["available_positions"]]
                
    if not positions:
        # Fallback to all active/non-disabled positions in the database
        sql = text("""
            SELECT description FROM position ORDER BY description ASC
        """)
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()
                positions = [r.description for r in rows]
        except Exception as e:
            print(f"Error fetching fallback positions: {e}")
            
    return JSONResponse({"status": "success", "data": positions})

@router.get("/clients")
def get_clients(request: Request):
    """Return all active clients for autocomplete"""
    clients = get_active_clients()
    return JSONResponse({"status": "success", "data": clients})

@router.get("/inbox")
def get_inbox_tickets(request: Request):
    """Return all pending email tickets"""
    state = _load_state()
    tickets = state.get("pending_tickets", [])
    
    # Extract unique client IDs
    client_ids = list({t.get("client_id") for t in tickets if t.get("client_id")})
    
    # Fetch current staffing managers from DB in batch
    from .knowledge_base import get_staffing_managers_for_clients
    managers_map, db_success = get_staffing_managers_for_clients(client_ids)
    
    # Only update cache if DB query succeeded to avoid clearing cache on network errors/downtime
    if db_success:
        updated = False
        for t in tickets:
            client_id = t.get("client_id")
            if client_id:
                current_sm = managers_map.get(client_id)
                if t.get("staffing_manager") != current_sm:
                    t["staffing_manager"] = current_sm
                    updated = True
            else:
                if t.get("staffing_manager") is not None:
                    t["staffing_manager"] = None
                    updated = True
                    
        if updated:
            from .email_monitor import _save_state
            state["pending_tickets"] = tickets
            _save_state(state)
            
    return JSONResponse({"status": "success", "data": tickets})


@router.post("/inbox/{msg_id}/dismiss")
def dismiss_inbox_ticket(request: Request, msg_id: str):
    """Remove a ticket from the inbox without processing it"""
    state = _load_state()
    tickets = state.get("pending_tickets", [])
    
    # Filter out the dismissed message
    new_tickets = [t for t in tickets if t.get("id") != msg_id]
    
    if len(tickets) == len(new_tickets):
        return JSONResponse({"status": "error", "message": "Ticket not found"}, status_code=404)
        
    state["pending_tickets"] = new_tickets
    _save_state(state)
    return JSONResponse({"status": "success", "message": "Ticket dismissed"})

@router.get("/employees/suggest")
def suggest_employees(request: Request, client_id: int, venue_name: str, position_name: str, date: str = ''):
    engine = _engine()
    
    # Query to find most frequently scheduled employees for this client + venue + position combo
    sql = text("""
        SELECT e.employee_id, u.first_name, u.last_name, COUNT(se.shift_employee_id) as freq,
        (
            SELECT COUNT(*)
            FROM shift_employee se_check
            JOIN shift_position sp_check ON se_check.shift_position_id = sp_check.shift_position_id
            JOIN shift s_check ON sp_check.shift_id = s_check.shift_id
            JOIN event ev_check ON s_check.event_id = ev_check.event_id
            WHERE se_check.employee_id = e.employee_id 
              AND ev_check.date = :date 
              AND se_check.deleted_at IS NULL
              AND se_check.cancel_reason = 0
              AND se_check.confirmed = 1
        ) as is_working
        FROM employee e
        JOIN user_party up ON e.employee_id = up.employee_id
        JOIN user u ON up.user_id = u.id
        JOIN shift_employee se ON se.employee_id = e.employee_id
        JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        JOIN position p ON sp.position_id = p.position_id
        JOIN shift s ON sp.shift_id = s.shift_id
        JOIN event ev ON s.event_id = ev.event_id
        JOIN venue v ON ev.venue_id = v.venue_id
        WHERE e.status IN (1, 10)
          AND e.deleted_at IS NULL
          AND se.deleted_at IS NULL
          AND se.confirmed = 1
          AND se.cancel_reason = 0
          AND ev.client_id = :client_id
          AND v.name = :venue_name
          AND p.description = :position_name
        GROUP BY e.employee_id, u.first_name, u.last_name
        ORDER BY freq DESC
        LIMIT 10
    """)
    
    with engine.connect() as conn:
        rows = conn.execute(sql, {
            "client_id": client_id,
            "venue_name": venue_name,
            "position_name": position_name,
            "date": date
        }).fetchall()
        
    results = []
    for row in rows:
        results.append({
            "id": row.employee_id,
            "name": f"{row.first_name} {row.last_name}",
            "is_working": row.is_working > 0,
            "freq": row.freq
        })
        
    return JSONResponse({"status": "success", "data": results})

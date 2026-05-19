from sqlalchemy import text
from apps.position_requests.scheduler import _engine
from datetime import datetime

def create_order(data: dict, user_id: int) -> dict:
    """
    Creates the necessary records in the GoLive database for the parsed order.
    - Resolves Venue to get state and county.
    - Creates Event(s)
    - Creates Shifts
    - Creates Shift Positions
    """
    try:
        basic = data.get('basic_information', {})
        shifts = data.get('shift_information', [])
        
        client_id = basic.get('client_id')
        venue_name = basic.get('venue_name')
        event_name = basic.get('event_name') or venue_name
        po_number = basic.get('purchase_order', '')
        
        if not client_id or not venue_name:
            return {"status": "error", "message": "Missing required basic information (client or venue)."}

        engine = _engine()
        with engine.begin() as conn:
            # 1. Resolve Venue ID, address, state, etc., default venue notes/timeclock, and client settings
            v_sql = text("""
                SELECT 
                    v.venue_id, v.address1, v.city, v.state, v.zip,
                    v.timeclock, v.timeclock_code_holder, v.timeclock_tolerance, v.timeclock_prestart_interval, v.timeclock_limit,
                    v.admin_notes, v.venue_details, v.description, 
                    COALESCE(CAST(v.parkings AS CHAR), v.parking) as parking, 
                    v.parking_note, v.directions, v.check_in,
                    v.county_id, c.no_break_penalty
                FROM venue v
                JOIN client c ON c.client_id = v.client_id
                WHERE v.client_id = :client_id AND v.name = :v_name 
                LIMIT 1
            """)
            venue = conn.execute(v_sql, {"client_id": client_id, "v_name": venue_name}).fetchone()
            
            if not venue:
                return {"status": "error", "message": f"Venue '{venue_name}' not found for this client."}
                
            (venue_id, address1, city, state, zip_code, 
             timeclock, tc_holder, tc_tol, tc_pre, tc_lim, 
             admin_notes, venue_details, description, parking, parking_note, directions, check_in,
             county_id, no_break_penalty) = venue
            
            # Since an order can span multiple days (and therefore multiple events), 
            # we group shifts by date. Each date gets its own Event.
            shifts_by_date = {}
            for s in shifts:
                dt = s.get('date')
                if not dt:
                    return {"status": "error", "message": "A shift is missing a date."}
                if dt not in shifts_by_date:
                    shifts_by_date[dt] = []
                shifts_by_date[dt].append(s)
                
            created_event_ids = []
            
            for event_date, day_shifts in shifts_by_date.items():
                
                # 2. Create Event (event represents a single day)
                ev_sql = text("""
                    INSERT INTO event (
                        client_id, venue_id, title, date, address1, city, state, zip, created_by,
                        timeclock, timeclock_code_holder, timeclock_tolerance, timeclock_prestart_interval, timeclock_limit,
                        admin_notes, venue_details, description, parking, parking_note, directions, check_in,
                        county_id, no_break_penalty
                    )
                    VALUES (
                        :client_id, :venue_id, :title, :date, :address1, :city, :state, :zip, :user_id,
                        :tc, :tc_holder, :tc_tol, :tc_pre, :tc_lim,
                        :admin_notes, :v_details, :desc, :parking, :parking_note, :directions, :check_in,
                        :county_id, :no_break_penalty
                    )
                """)
                res = conn.execute(ev_sql, {
                    "client_id": client_id,
                    "venue_id": venue_id,
                    "title": event_name,
                    "date": event_date,
                    "address1": address1,
                    "city": city,
                    "state": state,
                    "zip": zip_code,
                    "user_id": user_id,
                    "tc": timeclock or 'DISABLED',
                    "tc_holder": tc_holder,
                    "tc_tol": tc_tol,
                    "tc_pre": tc_pre,
                    "tc_lim": tc_lim or 0,
                    "admin_notes": admin_notes,
                    "v_details": venue_details,
                    "desc": description,
                    "parking": parking,
                    "parking_note": parking_note,
                    "directions": directions,
                    "check_in": check_in,
                    "county_id": county_id,
                    "no_break_penalty": no_break_penalty or 1
                })
                event_id = res.lastrowid
                created_event_ids.append(event_id)
                
                for s in day_shifts:
                    # Resolve position_id, along with current rates (checking effective date amounts), uniform, and tools
                    pos_name = s.get('position')
                    p_sql = text("""
                        SELECT 
                            p.position_id, 
                            COALESCE(
                                (SELECT vpa.pay_rate FROM venue_position_amount vpa WHERE vpa.venue_position_id = vp.venue_position_id AND (vpa.start_date IS NULL OR vpa.start_date <= :event_date) AND (vpa.end_date IS NULL OR vpa.end_date >= :event_date) ORDER BY vpa.id DESC LIMIT 1),
                                vp.del_rate, 
                                (SELECT cpa.pay_rate FROM client_position_amount cpa WHERE cpa.client_position_id = cp.id AND (cpa.start_date IS NULL OR cpa.start_date <= :event_date) AND (cpa.end_date IS NULL OR cpa.end_date >= :event_date) ORDER BY cpa.id DESC LIMIT 1),
                                cp.del_pay_rate, 
                                0
                            ) as pay_rate,
                            COALESCE(
                                (SELECT vpa.bill_rate FROM venue_position_amount vpa WHERE vpa.venue_position_id = vp.venue_position_id AND (vpa.start_date IS NULL OR vpa.start_date <= :event_date) AND (vpa.end_date IS NULL OR vpa.end_date >= :event_date) ORDER BY vpa.id DESC LIMIT 1),
                                vp.del_bill_rate, 
                                (SELECT cpa.bill_rate FROM client_position_amount cpa WHERE cpa.client_position_id = cp.id AND (cpa.start_date IS NULL OR cpa.start_date <= :event_date) AND (cpa.end_date IS NULL OR cpa.end_date >= :event_date) ORDER BY cpa.id DESC LIMIT 1),
                                cp.del_bill_rate, 
                                0
                            ) as bill_rate,
                            COALESCE(vp.del_uniform, cp.del_uniform_types, '') as uniform,
                            COALESCE(vp.del_tools, '') as tools,
                            COALESCE(vp.del_grooming_tools, '') as grooming_tools
                        FROM position p
                        LEFT JOIN client_position cp ON cp.position_id = p.position_id AND cp.client_id = :c_id
                        LEFT JOIN venue_position vp ON vp.position_id = p.position_id AND vp.venue_id = :v_id
                        WHERE p.description = :pos_name 
                        LIMIT 1
                    """)
                    pos_record = conn.execute(p_sql, {
                        "pos_name": pos_name,
                        "c_id": client_id,
                        "v_id": venue_id,
                        "event_date": event_date
                    }).fetchone()
                    
                    if not pos_record:
                        raise Exception(f"Position '{pos_name}' is invalid or not found.")
                    position_id, pay_rate, bill_rate, uniform, tools, grooming = pos_record
                    
                    shift_start = f"{event_date} {s['start_time']}:00"
                    shift_end = f"{event_date} {s['end_time']}:00"
                    
                    # 3. Create Shift
                    sh_sql = text("""
                        INSERT INTO shift (event_id, start, end, purchase_order)
                        VALUES (:ev_id, :start, :end, :po)
                    """)
                    s_res = conn.execute(sh_sql, {
                        "ev_id": event_id,
                        "start": shift_start,
                        "end": shift_end,
                        "po": po_number
                    })
                    shift_id = s_res.lastrowid
                    
                    # 4. Create Shift Position
                    sp_sql = text("""
                        INSERT INTO shift_position 
                        (shift_id, position_id, count, rate, base_rate, bill_rate, base_bill_rate, uniform, tools, grooming_tools)
                        VALUES 
                        (:s_id, :p_id, :count, :rate, :rate, :b_rate, :b_rate, :uniform, :tools, :grooming)
                    """)
                    conn.execute(sp_sql, {
                        "s_id": shift_id,
                        "p_id": position_id,
                        "count": s.get('staff_count', 1),
                        "rate": pay_rate,
                        "b_rate": bill_rate,
                        "uniform": uniform,
                        "tools": tools,
                        "grooming": grooming
                    })
                    
                    # Note: Grooming, Tools, Certifications, and Publishing links would go here.
                    # We will add them in sub-phases.

        return {"status": "success", "event_ids": created_event_ids, "message": f"Successfully published {len(created_event_ids)} events."}

    except Exception as e:
        print(f"Error publishing order: {e}")
        return {"status": "error", "message": str(e)}

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
        client_name = basic.get('client_name')
        venue_name = basic.get('venue_name')
        event_name = basic.get('event_name')
        if not event_name or event_name == client_name:
            event_name = venue_name
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
            
            # Get custom event address fields from basic information
            event_address1 = basic.get('event_address1', '').strip()
            event_address2 = basic.get('event_address2', '').strip()
            event_city = basic.get('event_city', '').strip()
            event_state = basic.get('event_state', '').strip()
            event_zip = basic.get('event_zip', '').strip()

            has_custom_address = bool(event_address1 or event_city or event_state or event_zip)
            has_address = 1 if has_custom_address else 0

            # Construct special_address string for legacy/compatibility support
            if has_custom_address:
                parts = [event_address1]
                if event_address2:
                    parts.append(event_address2)
                if event_city:
                    parts.append(event_city)
                if event_state or event_zip:
                    state_zip = " ".join(filter(None, [event_state, event_zip]))
                    parts.append(state_zip)
                special_address = ", ".join(parts)
            else:
                special_address = None

            final_address1 = event_address1 if event_address1 else address1
            final_address2 = event_address2 if event_address2 else None
            final_city = event_city if event_city else city
            final_state = event_state if event_state else state
            final_zip = event_zip if event_zip else zip_code

            # Geocode the address to resolve latitude and longitude
            from apps.similar_client_report.database import geocode_location
            import re

            # Clean address1 of any suite, apartment, unit, floor, building or # references for high geocoding accuracy
            suite_pat = re.compile(r'\b(suite|ste|apt|apartment|unit|room|rm|floor|fl|building|bldg)\b.*$|\s*#.*$', re.IGNORECASE)
            geocode_street = suite_pat.sub('', final_address1).strip()
            geocode_street = re.sub(r'[\s,]+$', '', geocode_street) # strip trailing spaces or commas

            # 1. Try full address (excluding address2/suite information)
            full_address_str = f"{geocode_street}, {final_city}, {final_state} {final_zip}"
            lat, lon = geocode_location(full_address_str)

            # 2. Fallback to city, state, zip level if full address geocoding fails
            if lat is None or lon is None:
                city_zip_str = f"{final_city}, {final_state} {final_zip}".strip()
                if city_zip_str:
                    lat, lon = geocode_location(city_zip_str)

            # 3. Fallback to city, state level if that fails
            if lat is None or lon is None:
                city_state_str = f"{final_city}, {final_state}".strip()
                if city_state_str:
                    lat, lon = geocode_location(city_state_str)

            latitude = lat if lat is not None else 0.0
            longitude = lon if lon is not None else 0.0

            for event_date, day_shifts in shifts_by_date.items():
                
                # Check if an event already exists for this client, venue, and date
                existing_ev_sql = text("""
                    SELECT event_id FROM event
                    WHERE client_id = :client_id 
                      AND venue_id = :venue_id 
                      AND date = :date 
                      AND deleted_at IS NULL
                    LIMIT 1
                """)
                existing_ev = conn.execute(existing_ev_sql, {
                    "client_id": client_id,
                    "venue_id": venue_id,
                    "date": event_date
                }).fetchone()
                
                has_create_shifts = any(s.get('action', 'CREATE') == 'CREATE' for s in day_shifts)
                if not has_create_shifts and not existing_ev:
                    raise Exception(f"No existing event found on {event_date} to update or remove shifts.")
                
                if existing_ev:
                    event_id = existing_ev[0]
                    created_event_ids.append(event_id)
                else:
                    # 2. Create Event (event represents a single day)
                    ev_sql = text("""
                        INSERT INTO event (
                            client_id, venue_id, title, date, address1, address2, city, state, zip, latitude, longitude, created_by,
                            timeclock, timeclock_code_holder, timeclock_tolerance, timeclock_prestart_interval, timeclock_limit,
                            admin_notes, venue_details, description, parking, parking_note, directions, check_in,
                            county_id, no_break_penalty, has_address, special_address
                        )
                        VALUES (
                            :client_id, :venue_id, :title, :date, :address1, :address2, :city, :state, :zip, :latitude, :longitude, :user_id,
                            :tc, :tc_holder, :tc_tol, :tc_pre, :tc_lim,
                            :admin_notes, :v_details, :desc, :parking, :parking_note, :directions, :check_in,
                            :county_id, :no_break_penalty, :has_address, :special_address
                        )
                    """)
                    res = conn.execute(ev_sql, {
                        "client_id": client_id,
                        "venue_id": venue_id,
                        "title": event_name,
                        "date": event_date,
                        "address1": final_address1,
                        "address2": final_address2,
                        "city": final_city,
                        "state": final_state,
                        "zip": final_zip,
                        "latitude": latitude,
                        "longitude": longitude,
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
                        "no_break_penalty": no_break_penalty or 1,
                        "has_address": has_address,
                        "special_address": special_address
                    })
                    event_id = res.lastrowid
                    created_event_ids.append(event_id)
                    
                    # 2.5 Pull and copy venue documents to event if visible to employees
                    vdoc_sql = text("""
                        SELECT vd.id, vd.filename, vd.description
                        FROM venue_document vd
                        JOIN document_type dt ON dt.id = vd.document_type_id
                        WHERE vd.venue_id = :venue_id
                          AND vd.deleted_at IS NULL
                          AND dt.visible_when IS NOT NULL
                          AND dt.visible_when LIKE '%EMPLOYEE%'
                    """)
                    vdocs = conn.execute(vdoc_sql, {"venue_id": venue_id}).fetchall()
                    for vd in vdocs:
                        conn.execute(text("""
                            INSERT INTO event_document (event_id, venue_document_id, filename, description, created_by)
                            VALUES (:event_id, :vdoc_id, :filename, :description, :user_id)
                        """), {
                            "event_id": event_id,
                            "vdoc_id": vd.id,
                            "filename": vd.filename,
                            "description": vd.description,
                            "user_id": user_id
                        })
                
                for s in day_shifts:
                    action = s.get('action', 'CREATE')
                    pos_name = s.get('position')
                    
                    if action in ('UPDATE', 'REMOVE'):
                        # Retrieve all active shifts/positions for this event to find a match
                        active_shifts_sql = text("""
                            SELECT 
                                s.shift_id, s.start, s.end, 
                                sp.shift_position_id, sp.position_id, p.description as position_name, sp.count
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
                                    
                        if not matched_shift:
                            raise Exception(f"Could not find an existing active '{pos_name}' shift on {event_date} to {action.lower()}.")
                            
                        shift_id = matched_shift.shift_id
                        shift_position_id = matched_shift.shift_position_id
                        
                        if action == 'REMOVE':
                            # Soft delete shift
                            conn.execute(text("""
                                UPDATE shift
                                SET deleted_at = NOW(), deleted_by = :user_id
                                WHERE shift_id = :shift_id
                            """), {"shift_id": shift_id, "user_id": user_id})
                            
                            # Soft delete shift position
                            conn.execute(text("""
                                UPDATE shift_position
                                SET deleted_at = NOW(), deleted_by = :user_id
                                WHERE shift_id = :shift_id AND deleted_at IS NULL
                            """), {"shift_id": shift_id, "user_id": user_id})
                            
                            # Soft delete shift employees
                            conn.execute(text("""
                                UPDATE shift_employee
                                SET deleted_at = NOW()
                                WHERE shift_position_id = :sp_id AND deleted_at IS NULL
                            """), {"sp_id": shift_position_id})
                            
                        elif action == 'UPDATE':
                            new_start = f"{event_date} {s['start_time']}:00"
                            new_end = f"{event_date} {s['end_time']}:00"
                            
                            current_start_str = matched_shift.start.strftime("%Y-%m-%d %H:%M:%S") if hasattr(matched_shift.start, 'strftime') else str(matched_shift.start) if matched_shift.start else ""
                            current_end_str = matched_shift.end.strftime("%Y-%m-%d %H:%M:%S") if hasattr(matched_shift.end, 'strftime') else str(matched_shift.end) if matched_shift.end else ""
                            
                            time_changed = (current_start_str != new_start or current_end_str != new_end)
                            
                            if time_changed:
                                # Check if has active employees
                                has_emp = conn.execute(text("""
                                    SELECT COUNT(*) FROM shift_employee
                                    WHERE shift_position_id = :sp_id
                                      AND deleted_at IS NULL
                                      AND (cancel_reason IS NULL OR cancel_reason = 0)
                                """), {"sp_id": shift_position_id}).scalar()
                                
                                if has_emp > 0:
                                    conn.execute(text("""
                                        UPDATE shift
                                        SET start = :start, end = :end, old_start = :old_start, old_end = :old_end
                                        WHERE shift_id = :shift_id
                                    """), {
                                        "start": new_start,
                                        "end": new_end,
                                        "old_start": matched_shift.start,
                                        "old_end": matched_shift.end,
                                        "shift_id": shift_id
                                    })
                                    
                                    # Reset confirmed and request_by
                                    conn.execute(text("""
                                        UPDATE shift_employee
                                        SET confirmed = 0, request_by = :user_id
                                        WHERE shift_position_id = :sp_id
                                          AND deleted_at IS NULL
                                          AND (cancel_reason IS NULL OR cancel_reason = 0)
                                    """), {"sp_id": shift_position_id, "user_id": user_id})
                                else:
                                    conn.execute(text("""
                                        UPDATE shift
                                        SET start = :start, end = :end
                                        WHERE shift_id = :shift_id
                                    """), {
                                        "start": new_start,
                                        "end": new_end,
                                        "shift_id": shift_id
                                    })
                                    
                            new_count = int(s.get('staff_count', matched_shift.count))
                            if new_count != matched_shift.count:
                                conn.execute(text("""
                                    UPDATE shift_position
                                    SET count = :count
                                    WHERE shift_position_id = :sp_id
                                """), {"count": new_count, "sp_id": shift_position_id})
                        
                        continue
                        
                    # Resolve position_id, along with current rates (checking effective date amounts), uniform, and tools
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
                            COALESCE(vp.del_uniform, vp.del_uniform_types, cp.del_uniform_types, '') as uniform,
                            COALESCE(vp.del_tools, '') as tools,
                            COALESCE(vp.del_grooming_tools, '') as grooming_tools,
                            vp.venue_position_id,
                            cp.id as client_position_id
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
                    position_id, pay_rate, bill_rate, uniform, tools, grooming, venue_position_id, client_position_id = pos_record
                    
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
                    sp_res = conn.execute(sp_sql, {
                        "s_id": shift_id,
                        "p_id": position_id,
                        "count": s.get('staff_count', 1),
                        "rate": pay_rate,
                        "b_rate": bill_rate,
                        "uniform": uniform,
                        "tools": tools,
                        "grooming": grooming
                    })
                    shift_position_id = sp_res.lastrowid
                    
                    # 4.5.1 Copy Uniforms hierarchically (venue -> client -> position default)
                    uniforms = []
                    if venue_position_id:
                        uniforms = conn.execute(text("""
                            SELECT vpu.uniform_id, u.name 
                            FROM venue_position_uniform vpu
                            JOIN uniform u ON u.id = vpu.uniform_id
                            WHERE vpu.venue_position_id = :vp_id
                        """), {"vp_id": venue_position_id}).fetchall()
                    
                    if not uniforms and client_position_id:
                        uniforms = conn.execute(text("""
                            SELECT cpu.uniform_id, u.name 
                            FROM client_position_uniform cpu
                            JOIN uniform u ON u.id = cpu.uniform_id
                            WHERE cpu.client_position_id = :cp_id
                        """), {"cp_id": client_position_id}).fetchall()
                        
                    if not uniforms:
                        uniforms = conn.execute(text("""
                            SELECT pu.uniform_id, u.name 
                            FROM position_uniform pu
                            JOIN uniform u ON u.id = pu.uniform_id
                            WHERE pu.position_id = :pos_id
                        """), {"pos_id": position_id}).fetchall()
                        
                    uniform_names = []
                    for uni in uniforms:
                        uniform_names.append(uni.name)
                        conn.execute(text("""
                            INSERT INTO shift_position_uniform (shift_position_id, uniform_id, created_by)
                            VALUES (:sp_id, :uniform_id, :user_id)
                        """), {
                            "sp_id": shift_position_id,
                            "uniform_id": uni.uniform_id,
                            "user_id": user_id
                        })
                    
                    # 4.5.2 Copy Tools hierarchically (venue -> client -> position default)
                    tools_list = []
                    if venue_position_id:
                        tools_list = conn.execute(text("""
                            SELECT vpt.tool_id, t.name 
                            FROM venue_position_tool vpt
                            JOIN tool t ON t.id = vpt.tool_id
                            WHERE vpt.venue_position_id = :vp_id
                        """), {"vp_id": venue_position_id}).fetchall()
                    
                    if not tools_list and client_position_id:
                        tools_list = conn.execute(text("""
                            SELECT cpt.tool_id, t.name 
                            FROM client_position_tool cpt
                            JOIN tool t ON t.id = cpt.tool_id
                            WHERE cpt.client_position_id = :cp_id
                        """), {"cp_id": client_position_id}).fetchall()
                        
                    if not tools_list:
                        tools_list = conn.execute(text("""
                            SELECT pt.tool_id, t.name 
                            FROM position_tool pt
                            JOIN tool t ON t.id = pt.tool_id
                            WHERE pt.position_id = :pos_id
                        """), {"pos_id": position_id}).fetchall()
                        
                    tool_names = []
                    for t_item in tools_list:
                        tool_names.append(t_item.name)
                        conn.execute(text("""
                            INSERT INTO shift_position_tool (shift_position_id, tool_id, created_by)
                            VALUES (:sp_id, :tool_id, :user_id)
                        """), {
                            "sp_id": shift_position_id,
                            "tool_id": t_item.tool_id,
                            "user_id": user_id
                        })
                        
                    # 4.5.3 Copy Grooming Tools hierarchically (venue -> client -> position default)
                    grooming_list = []
                    if venue_position_id:
                        grooming_list = conn.execute(text("""
                            SELECT vpg.grooming_tool_id, g.name 
                            FROM venue_position_grooming_tool vpg
                            JOIN grooming_tool g ON g.id = vpg.grooming_tool_id
                            WHERE vpg.venue_position_id = :vp_id
                        """), {"vp_id": venue_position_id}).fetchall()
                    
                    if not grooming_list and client_position_id:
                        grooming_list = conn.execute(text("""
                            SELECT cpg.grooming_tool_id, g.name 
                            FROM client_position_grooming_tool cpg
                            JOIN grooming_tool g ON g.id = cpg.grooming_tool_id
                            WHERE cpg.client_position_id = :cp_id
                        """), {"cp_id": client_position_id}).fetchall()
                        
                    if not grooming_list:
                        grooming_list = conn.execute(text("""
                            SELECT pg.grooming_tool_id, g.name 
                            FROM position_grooming_tool pg
                            JOIN grooming_tool g ON g.id = pg.grooming_tool_id
                            WHERE pg.position_id = :pos_id
                        """), {"pos_id": position_id}).fetchall()
                        
                    grooming_names = []
                    for g_item in grooming_list:
                        grooming_names.append(g_item.name)
                        conn.execute(text("""
                            INSERT INTO shift_position_grooming_tool (shift_position_id, grooming_tool_id, created_by)
                            VALUES (:sp_id, :grooming_tool_id, :user_id)
                        """), {
                            "sp_id": shift_position_id,
                            "grooming_tool_id": g_item.grooming_tool_id,
                            "user_id": user_id
                        })
                        
                    # 4.5.4 Copy Certifications hierarchically (venue -> client -> position default)
                    certs = []
                    if venue_position_id:
                        certs = conn.execute(text("""
                            SELECT certification_id 
                            FROM venue_position_certification 
                            WHERE venue_position_id = :vp_id
                        """), {"vp_id": venue_position_id}).fetchall()
                    
                    if not certs and client_position_id:
                        certs = conn.execute(text("""
                            SELECT certification_id 
                            FROM client_position_certification 
                            WHERE client_position_id = :cp_id
                        """), {"cp_id": client_position_id}).fetchall()
                        
                    if not certs:
                        certs = conn.execute(text("""
                            SELECT certification_id 
                            FROM position_certification 
                            WHERE position_id = :pos_id
                        """), {"pos_id": position_id}).fetchall()
                        
                    for cert in certs:
                        conn.execute(text("""
                            INSERT INTO shift_position_certification (shift_position_id, certification_id, created_by)
                            VALUES (:sp_id, :cert_id, :user_id)
                        """), {
                            "sp_id": shift_position_id,
                            "cert_id": cert.certification_id,
                            "user_id": user_id
                        })

                    # 4.5.5 Update shift_position text fields with dynamic comma-separated summaries
                    if uniform_names or tool_names or grooming_names:
                        conn.execute(text("""
                            UPDATE shift_position
                            SET 
                                uniform = :uniform,
                                tools = :tools,
                                grooming_tools = :grooming
                            WHERE shift_position_id = :sp_id
                        """), {
                            "uniform": ", ".join(uniform_names) if uniform_names else "",
                            "tools": ", ".join(tool_names) if tool_names else "",
                            "grooming": ", ".join(grooming_names) if grooming_names else "",
                            "sp_id": shift_position_id
                        })
                        
                    # 4.6 Handle Publication and Employee Requests
                    pub_rule = s.get('publication_rule', 'DO_NOT_PUBLISH')
                    req_emp_id = s.get('requested_employee_id', '')

                    if pub_rule in ['ALL', 'PREFERRED', 'WORKED_BEFORE']:
                        # Create Publishing Record
                        pub_sql = text("""
                            INSERT INTO publishing (event_id, client_id, `to`, created_by, begin, employee_statuses)
                            VALUES (:event_id, :client_id, :to_val, :user_id, :begin_val, :emp_statuses)
                        """)
                        pub_res = conn.execute(pub_sql, {
                            "event_id": event_id,
                            "client_id": client_id,
                            "to_val": pub_rule,
                            "user_id": user_id,
                            "begin_val": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "emp_statuses": '[1,10,2,6]'
                        })
                        publishing_id = pub_res.lastrowid

                        # Link Shift to Publishing
                        pub_shift_sql = text("""
                            INSERT INTO publishing_shift (publishing_id, shift_id, created_by)
                            VALUES (:pub_id, :shift_id, :created_by)
                        """)
                        conn.execute(pub_shift_sql, {
                            "pub_id": publishing_id,
                            "shift_id": shift_id,
                            "created_by": user_id or 35598
                        })

                    elif pub_rule == 'REQUEST_EMPLOYEE' and req_emp_id and req_emp_id.isdigit():
                        # Create unconfirmed shift_employee record for the requested employee
                        se_sql = text("""
                            INSERT INTO shift_employee (
                                event_id, shift_position_id, employee_id, request_by,
                                rate, bill_rate, confirmed, cancel_reason
                            )
                            VALUES (
                                :event_id, :sp_id, :emp_id, :user_id,
                                :rate, :bill_rate, 0, 0
                            )
                        """)
                        conn.execute(se_sql, {
                            "event_id": event_id,
                            "sp_id": shift_position_id,
                            "emp_id": int(req_emp_id),
                            "user_id": user_id,
                            "rate": pay_rate,
                            "bill_rate": bill_rate
                        })
                
                # 5. Update Event Stat Columns to color-code event correctly in GoLive (red/gray/white)
                update_stats_sql = text("""
                    UPDATE event e
                    SET 
                        e.stat_shifts_count = (
                            SELECT COUNT(*) 
                            FROM shift s 
                            WHERE s.event_id = e.event_id 
                              AND s.deleted_at IS NULL
                        ),
                        e.stat_shifts_positions_count = (
                            SELECT COALESCE(SUM(sp.count), 0)
                            FROM shift s
                            JOIN shift_position sp ON sp.shift_id = s.shift_id
                            WHERE s.event_id = e.event_id
                              AND s.deleted_at IS NULL
                              AND sp.deleted_at IS NULL
                        ),
                        e.stat_shifts_filled_count = (
                            SELECT COUNT(*)
                            FROM shift s
                            JOIN shift_position sp ON sp.shift_id = s.shift_id
                            JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id
                            WHERE s.event_id = e.event_id
                              AND s.deleted_at IS NULL
                              AND sp.deleted_at IS NULL
                              AND se.deleted_at IS NULL
                        ),
                        e.stat_shifts_filled_confirmed_count = (
                            SELECT COUNT(*)
                            FROM shift s
                            JOIN shift_position sp ON sp.shift_id = s.shift_id
                            JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id
                            WHERE s.event_id = e.event_id
                              AND s.deleted_at IS NULL
                              AND sp.deleted_at IS NULL
                              AND se.deleted_at IS NULL
                              AND se.confirmed = 1
                        ),
                        e.stat_employees_count = (
                            SELECT COUNT(*)
                            FROM shift s
                            JOIN shift_position sp ON sp.shift_id = s.shift_id
                            JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id
                            WHERE s.event_id = e.event_id
                              AND s.deleted_at IS NULL
                              AND sp.deleted_at IS NULL
                              AND se.deleted_at IS NULL
                        ),
                        e.stat_employees_confirmed_count = (
                            SELECT COUNT(*)
                            FROM shift s
                            JOIN shift_position sp ON sp.shift_id = s.shift_id
                            JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id
                            WHERE s.event_id = e.event_id
                              AND s.deleted_at IS NULL
                              AND sp.deleted_at IS NULL
                              AND se.deleted_at IS NULL
                              AND se.confirmed = 1
                        ),
                        e.stat_filled = IF(
                            (
                                SELECT COALESCE(SUM(sp.count), 0) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL
                            ) > 0
                            AND 
                            (
                                SELECT COALESCE(SUM(sp.count), 0) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL
                            )
                            =
                            (
                                SELECT COUNT(*) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL 
                                  AND se.deleted_at IS NULL
                            ),
                            1, 0
                        ),
                        e.stat_filled_confirmed = IF(
                            (
                                SELECT COALESCE(SUM(sp.count), 0) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL
                            ) > 0
                            AND 
                            (
                                SELECT COALESCE(SUM(sp.count), 0) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL
                            )
                            =
                            (
                                SELECT COUNT(*) 
                                FROM shift s 
                                JOIN shift_position sp ON sp.shift_id = s.shift_id 
                                JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id 
                                WHERE s.event_id = e.event_id 
                                  AND s.deleted_at IS NULL 
                                  AND sp.deleted_at IS NULL 
                                  AND se.deleted_at IS NULL
                                  AND se.confirmed = 1
                            ),
                            1, 0
                        )
                    WHERE e.event_id = :event_id
                """)
                conn.execute(update_stats_sql, {"event_id": event_id})

        return {"status": "success", "event_ids": created_event_ids, "message": f"Successfully published {len(created_event_ids)} events."}

    except Exception as e:
        print(f"Error publishing order: {e}")
        return {"status": "error", "message": str(e)}

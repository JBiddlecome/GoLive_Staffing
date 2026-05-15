# apps/certificate_approver/scheduler.py

import asyncio
from datetime import datetime

_auto_approve_enabled = False
_state_initialized = False

def _ensure_state_table():
    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS golive_app_state (
                    app_name VARCHAR(100) NOT NULL,
                    state_key VARCHAR(100) NOT NULL,
                    state_value LONGTEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (app_name, state_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """))
    except Exception as e:
        print(f"[Certificate Approver] Error ensuring state table: {e}")
    finally:
        engine.dispose()

def _ensure_ai_reviewed_column():
    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE employee_certification ADD COLUMN ai_reviewed_at DATETIME DEFAULT NULL"))
            print("[Certificate Approver] Added ai_reviewed_at column.")
    except Exception as e:
        # Expected to fail if column already exists
        pass
    finally:
        engine.dispose()

def get_auto_approve_enabled() -> bool:
    global _auto_approve_enabled, _state_initialized
    if not _state_initialized:
        _ensure_state_table()
        _ensure_ai_reviewed_column()
        from sqlalchemy import text
        from apps.position_requests.scheduler import _engine
        engine = _engine()
        try:
            with engine.begin() as conn:
                res = conn.execute(text("SELECT state_value FROM golive_app_state WHERE app_name = 'certificate_approver' AND state_key = 'auto_approve'")).fetchone()
                if res and res[0] is not None:
                    _auto_approve_enabled = (res[0].lower() == 'true')
        except Exception as e:
            print(f"[Certificate Approver] Error loading auto approve state from DB: {e}")
        finally:
            engine.dispose()
        _state_initialized = True
    return _auto_approve_enabled

def set_auto_approve_enabled(enabled: bool):
    global _auto_approve_enabled, _state_initialized
    _auto_approve_enabled = enabled
    _state_initialized = True
    _ensure_state_table()
    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    try:
        with engine.begin() as conn:
            val_str = 'true' if enabled else 'false'
            conn.execute(text("""
                INSERT INTO golive_app_state (app_name, state_key, state_value)
                VALUES ('certificate_approver', 'auto_approve', :value)
                ON DUPLICATE KEY UPDATE state_value = :value
            """), {"value": val_str})
    except Exception as e:
        print(f"[Certificate Approver] Error saving auto approve state to DB: {e}")
    finally:
        engine.dispose()

async def certificate_approval_loop():
    from apps.certificate_approver.views import get_pending_certificates, analyze_certificate_ai, approve_cert_action, deny_cert_action
    
    while True:
        try:
            if not get_auto_approve_enabled():
                await asyncio.sleep(60)
                continue
                
            print(f"[{datetime.now()}] Certificate Approver: Checking for pending certificates...")
            pending = get_pending_certificates(include_ai_reviewed=False)
            
            if not pending:
                await asyncio.sleep(60)
                continue
                
            for cert in pending:
                if not get_auto_approve_enabled():
                    break
                    
                print(f"[{datetime.now()}] Certificate Approver: Analyzing cert {cert['id']} for {cert['first_name']} {cert['last_name']}...")
                
                # Perform AI analysis
                employee_name = f"{cert['first_name']} {cert['last_name']}".strip()
                result = await analyze_certificate_ai(
                    cert_url=cert['cert_url'],
                    cert_type_id=cert['cert_type_id'],
                    cert_type_name=cert['cert_type_name'],
                    issued_at=cert['issued_at'] or "",
                    expires_at=cert['expires_at'] or "",
                    number=cert['number'] or "",
                    file_name=cert['file_name'],
                    employee_name=employee_name
                )
                
                if result.get("status") == "success":
                    analysis = result.get("ai_analysis", {})
                    decision = analysis.get("decision")
                    extracted = analysis.get("extracted", {})
                    
                    if decision == "APPROVE":
                        issued_val = extracted.get("issue_date") or cert['issued_at'] or ""
                        expires_val = extracted.get("expiration_date") or cert['expires_at'] or ""
                        number_val = extracted.get("certificate_number") or cert['number'] or ""
                        print(f"[{datetime.now()}] Certificate Approver: Approving cert {cert['id']}")
                        
                        approve_cert_action(
                            record_id=cert['id'],
                            first_name=cert['first_name'],
                            email=cert['email'],
                            cert_type_name=cert['cert_type_name'],
                            issued_at=issued_val,
                            expires_at=expires_val,
                            number=number_val
                        )
                        
                        # Handle Other Work Registration automatically if eligible
                        if cert.get('other_work_eligible') and cert.get('employee_id') and cert.get('other_work_type_id'):
                            from apps.certificate_approver.views import register_other_work_for_cert
                            register_other_work_for_cert(cert['employee_id'], cert['other_work_type_id'], issued_val)
                            
                    elif decision == "DECLINE":
                        reasons = analysis.get("reasons", [])
                        reason_text = ", ".join(reasons) if reasons else "Does not meet requirements."
                        print(f"[{datetime.now()}] Certificate Approver: AI chose to decline cert {cert['id']}: {reason_text}. Keeping in app for manual review.")
                        # Temporarily disabled automatic denials until AI is verified to be 100% accurate
                        # deny_cert_action(
                        #     record_id=cert['id'],
                        #     file_name=cert['file_name'],
                        #     first_name=cert['first_name'],
                        #     email=cert['email'],
                        #     cert_type_name=cert['cert_type_name'],
                        #     reason=reason_text
                        # )
                    else:
                        print(f"[{datetime.now()}] Certificate Approver: Cert {cert['id']} needs manual review. Skipping.")
                        
                    # Mark as AI reviewed to prevent infinite loops
                    from sqlalchemy import text
                    from apps.position_requests.scheduler import _engine
                    try:
                        eng = _engine()
                        with eng.begin() as conn:
                            conn.execute(text("UPDATE employee_certification SET ai_reviewed_at = NOW() WHERE id = :rid"), {"rid": cert['id']})
                        eng.dispose()
                    except Exception as e:
                        print(f"Failed to mark ai_reviewed_at for cert {cert['id']}: {e}")
                else:
                    print(f"[{datetime.now()}] Certificate Approver: AI Error on cert {cert['id']}")
                
                # Sleep briefly between certs to avoid rate limits
                await asyncio.sleep(5)
                
            # Sleep before checking again
            await asyncio.sleep(120)
            
        except Exception as e:
            print(f"Certificate approval loop error: {e}")
            await asyncio.sleep(60)

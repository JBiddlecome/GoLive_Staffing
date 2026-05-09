# apps/certificate_approver/scheduler.py

import asyncio
from datetime import datetime

_auto_approve_enabled = False

def get_auto_approve_enabled() -> bool:
    global _auto_approve_enabled
    return _auto_approve_enabled

def set_auto_approve_enabled(enabled: bool):
    global _auto_approve_enabled
    _auto_approve_enabled = enabled

async def certificate_approval_loop():
    from apps.certificate_approver.views import get_pending_certificates, analyze_certificate_ai, approve_cert_action, deny_cert_action
    
    while True:
        try:
            if not _auto_approve_enabled:
                await asyncio.sleep(60)
                continue
                
            print(f"[{datetime.now()}] Certificate Approver: Checking for pending certificates...")
            pending = get_pending_certificates()
            
            if not pending:
                await asyncio.sleep(60)
                continue
                
            for cert in pending:
                if not _auto_approve_enabled:
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
                else:
                    print(f"[{datetime.now()}] Certificate Approver: AI Error on cert {cert['id']}")
                
                # Sleep briefly between certs to avoid rate limits
                await asyncio.sleep(5)
                
            # Sleep before checking again
            await asyncio.sleep(120)
            
        except Exception as e:
            print(f"Certificate approval loop error: {e}")
            await asyncio.sleep(60)

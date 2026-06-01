import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from apps.profile_picture_approval.views import (
    get_pending_photos,
    analyze_photo_ai,
    approve_photo_action,
    deny_photo_action
)

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
        print(f"[Profile Picture Auto-Approve] Error ensuring state table: {e}")
    finally:
        engine.dispose()

def get_auto_approve_enabled() -> bool:
    global _auto_approve_enabled, _state_initialized
    if not _state_initialized:
        _ensure_state_table()
        from sqlalchemy import text
        from apps.position_requests.scheduler import _engine
        engine = _engine()
        try:
            with engine.begin() as conn:
                res = conn.execute(text("SELECT state_value FROM golive_app_state WHERE app_name = 'profile_picture_approval' AND state_key = 'auto_approve'")).fetchone()
                if res and res[0] is not None:
                    _auto_approve_enabled = (res[0].lower() == 'true')
        except Exception as e:
            print(f"[Profile Picture Auto-Approve] Error loading auto approve state from DB: {e}")
        finally:
            engine.dispose()
        _state_initialized = True
    return _auto_approve_enabled

def set_auto_approve_enabled(value: bool) -> None:
    global _auto_approve_enabled, _state_initialized
    _auto_approve_enabled = value
    _state_initialized = True
    _ensure_state_table()
    from sqlalchemy import text
    from apps.position_requests.scheduler import _engine
    engine = _engine()
    try:
        with engine.begin() as conn:
            val_str = 'true' if value else 'false'
            conn.execute(text("""
                INSERT INTO golive_app_state (app_name, state_key, state_value)
                VALUES ('profile_picture_approval', 'auto_approve', :value)
                ON DUPLICATE KEY UPDATE state_value = :value
            """), {"value": val_str})
    except Exception as e:
        print(f"[Profile Picture Auto-Approve] Error saving auto approve state to DB: {e}")
    finally:
        engine.dispose()
    print(f"[Profile Picture Auto-Approve] Toggle set to {'ON' if value else 'OFF'}")

async def profile_picture_approval_loop():
    """Background loop – polls every 2 minutes."""
    await asyncio.sleep(15)  # brief startup stagger
    
    if os.getenv("RENDER", "").lower() != "true":
        print(
            "[Profile Picture Auto-Approve] Local environment detected (RENDER=true missing). "
            "Running locally for testing purposes."
        )

    print("[Profile Picture Auto-Approve] Monitor started.")

    while True:
        try:
            if not get_auto_approve_enabled():
                # Just sleep if not enabled
                await asyncio.sleep(120)
                continue
                
            photos = get_pending_photos()
            if photos:
                print(f"[Profile Picture Auto-Approve] Found {len(photos)} pending photos.")
                
            for photo in photos:
                if not get_auto_approve_enabled():
                    break # Stop if disabled mid-processing
                    
                print(f"[Profile Picture Auto-Approve] Analyzing {photo['file_name']} for {photo['first_name']} {photo['last_name']}")
                
                analysis_result = await analyze_photo_ai(photo['photo_url'])
                
                if analysis_result.get("status") == "success":
                    ai_data = analysis_result.get("ai_analysis", {})
                    suitable = ai_data.get("suitable", False)
                    reason = ai_data.get("reason", "No reason provided")
                    
                    if suitable:
                        print(f"[Profile Picture Auto-Approve] Approving photo {photo['file_name']}.")
                        success, err = approve_photo_action(
                            photo['employee_id'],
                            photo['file_name'],
                            photo['first_name'],
                            photo['email']
                        )
                        if not success:
                            print(f"[Profile Picture Auto-Approve] Failed to approve: {err}")
                    else:
                        print(f"[Profile Picture Auto-Approve] Photo {photo['file_name']} not suitable. Reason: {reason}. Auto-denying photo.")
                        success, err = deny_photo_action(
                            photo['employee_id'],
                            photo['file_name'],
                            photo['first_name'],
                            photo['email']
                        )
                        if not success:
                            print(f"[Profile Picture Auto-Approve] Failed to deny: {err}")
                else:
                    print(f"[Profile Picture Auto-Approve] AI Analysis failed: {analysis_result.get('message')}")
                    
        except Exception as e:
            print(f"[Profile Picture Auto-Approve] Exception in loop: {e}")

        await asyncio.sleep(120)  # poll every 2 minutes

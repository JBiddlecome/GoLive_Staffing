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

_STATE_FILE = Path("apps/profile_picture_approval/auto_approve_state.json")

def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            with open(_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"enabled": False}

def _save_state(state: dict) -> None:
    try:
        tmp = _STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        tmp.replace(_STATE_FILE)
    except Exception as e:
        print(f"[Profile Picture Auto-Approve] Error saving state: {e}")

def get_auto_approve_enabled() -> bool:
    state = _load_state()
    return state.get("enabled", False)

def set_auto_approve_enabled(value: bool) -> None:
    state = _load_state()
    state["enabled"] = value
    _save_state(state)
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
                        print(f"[Profile Picture Auto-Approve] Photo {photo['file_name']} not suitable. Reason: {reason}. Leaving for manual review.")
                else:
                    print(f"[Profile Picture Auto-Approve] AI Analysis failed: {analysis_result.get('message')}")
                    
        except Exception as e:
            print(f"[Profile Picture Auto-Approve] Exception in loop: {e}")

        await asyncio.sleep(120)  # poll every 2 minutes

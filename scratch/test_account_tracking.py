import asyncio
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set dummy environment variables to bypass actual DB connection issues if any,
# since we are only testing the SQLite tracking database.
os.environ["DB_HOST"] = "localhost"
os.environ["DB_USER"] = "root"
os.environ["DB_PASSWORD"] = "password"

from apps.profit_tracker.views import (
    init_tracking_db,
    _get_db_conn,
    set_client_owner,
    set_client_stuck_reason,
    set_client_next_action,
    complete_client_next_action,
    get_account_tracking,
    SetOwnerPayload,
    SetStuckReasonPayload,
    SetNextActionPayload,
    CompleteNextActionPayload,
)

async def test_workflow():
    print("--- Starting Account Tracking System Tests ---")
    
    # 1. Initialize the database
    init_tracking_db()
    print("[1/6] Database initialized successfully.")
    
    test_client = "---TEST_CLIENT---"
    
    # Clean up any leftover test data
    conn = _get_db_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM client_profile WHERE client_name = ?", (test_client,))
    cursor.execute("DELETE FROM client_next_actions WHERE client_name = ?", (test_client,))
    conn.commit()
    conn.close()
    
    # 2. Test Set Owner
    owner_payload = SetOwnerPayload(client_name=test_client, owner="Dan Stone")
    response = await set_client_owner(owner_payload)
    print(f"[2/6] Set Owner Response: {response.body.decode()}")
    assert b"ok" in response.body, "Failed to set owner"
    
    # 3. Test Set Stuck Reason
    stuck_payload = SetStuckReasonPayload(client_name=test_client, stuck_reason="Waiting on payroll approval")
    response = await set_client_stuck_reason(stuck_payload)
    print(f"[3/6] Set Stuck Reason Response: {response.body.decode()}")
    assert b"ok" in response.body, "Failed to set stuck reason"
    
    # 4. Test Set Next Action
    action_payload = SetNextActionPayload(client_name=test_client, action_text="Send email to follow up on Friday")
    response = await set_client_next_action(action_payload)
    response_json = response.body.decode()
    print(f"[4/6] Set Next Action Response: {response_json}")
    assert b"ok" in response.body, "Failed to set next action"
    
    import json
    action_data = json.loads(response_json)
    action_id = action_data.get("id")
    assert action_id is not None, "Action ID was not returned"
    
    # 5. Test Get Account Tracking
    response = await get_account_tracking()
    tracking_data = json.loads(response.body.decode())
    print("[5/6] Retrieved Account Tracking data.")
    assert test_client in tracking_data, "Test client not found in tracking data"
    client_data = tracking_data[test_client]
    assert client_data["owner"] == "Dan Stone", f"Expected owner 'Dan Stone', got '{client_data['owner']}'"
    assert client_data["stuck_reason"] == "Waiting on payroll approval", f"Expected stuck reason, got '{client_data['stuck_reason']}'"
    assert client_data["active_next_action"] is not None, "Active next action is missing"
    assert client_data["active_next_action"]["action_text"] == "Send email to follow up on Friday", "Incorrect action text"
    
    # 6. Test Complete Next Action
    complete_payload = CompleteNextActionPayload(action_id=action_id)
    response = await complete_client_next_action(complete_payload)
    print(f"[6/6] Complete Next Action Response: {response.body.decode()}")
    assert b"ok" in response.body, "Failed to complete next action"
    
    # Verify that the active next action is cleared
    response = await get_account_tracking()
    tracking_data = json.loads(response.body.decode())
    client_data = tracking_data.get(test_client, {})
    assert client_data.get("active_next_action") is None, "Active next action should be None after completion"
    
    # Clean up test data
    conn = _get_db_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM client_profile WHERE client_name = ?", (test_client,))
    cursor.execute("DELETE FROM client_next_actions WHERE client_name = ?", (test_client,))
    conn.commit()
    conn.close()
    
    print("\n--- ALL TESTS PASSED SUCCESSFULLY! ---")

if __name__ == "__main__":
    asyncio.run(test_workflow())

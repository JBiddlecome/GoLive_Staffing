import os
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.employee_phone_county_audit.views import _audit_employee_list

def test_audit_flow():
    # Construct a minimal pandas DataFrame that simulates the uploaded Employee List workbook.
    # The 'Employee ID' column in the workbook actually holds payroll_ids (often as decimals).
    # Required columns: "Status", "Employee ID", "Mobile", "County of Residence", "First Name", "Last Name", "Email"
    data = {
        "Status": ["Active", "Active", "Active", "Active"],
        "Employee ID": [112492.0, 15108, "109063", "999999.0"],  # mock payroll_ids
        "Mobile": ["172-742-6486", "181-842-7040", "166-921-9970", "155-555-5555"],
        "County of Residence": ["Orange", "Los Angeles", "Orange", "San Diego"],
        "First Name": ["John", "Safoa", "Jack", "Nonexistent"],
        "Last Name": ["Doe", "Abboa-Offei", "Todd", "Employee"],
        "Email": ["john.doe@example.com", "safoa@example.com", "jack@example.com", "none@example.com"]
    }
    df = pd.DataFrame(data)
    
    # Save to Excel bytes
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    excel_bytes = out.getvalue()
    
    # Run the audit
    res = _audit_employee_list(excel_bytes)
    mobile_df = res["mobile"]
    print("Mobile Issues Dataframe Columns:", mobile_df.columns.tolist())
    print("Mobile Issues Row Data:")
    records = mobile_df.to_dict(orient="records")
    for r in records:
        print(r)
    
    # Check that "Original Phone" column exists
    assert "Original Phone" in mobile_df.columns, "'Original Phone' column missing!"
    
    # Verify employee 112492 (database employee_id 46648)
    row_112492 = mobile_df[mobile_df["Employee ID"] == "112492"].iloc[0]
    val_112492 = row_112492["Original Phone"]
    print(f"Extracted 'Original Phone' for payroll 112492: {val_112492}")
    assert val_112492 == "727-426-4865", f"Expected '727-426-4865' but got {val_112492}"
    
    # Verify employee 15108 (database employee_id 15106)
    row_15108 = mobile_df[mobile_df["Employee ID"] == "15108"].iloc[0]
    val_15108 = row_15108["Original Phone"]
    print(f"Extracted 'Original Phone' for payroll 15108: {val_15108}")
    assert val_15108 == "818-427-0402", f"Expected '818-427-0402' but got {val_15108}"

    # Verify employee 109063 (database employee_id 34962)
    row_109063 = mobile_df[mobile_df["Employee ID"] == "109063"].iloc[0]
    val_109063 = row_109063["Original Phone"]
    print(f"Extracted 'Original Phone' for payroll 109063: {val_109063}")
    assert val_109063 == "669-219-9702", f"Expected '669-219-9702' but got {val_109063}"

    # Verify employee 999999 (nonexistent in history_entry)
    row_999999 = mobile_df[mobile_df["Employee ID"] == "999999"].iloc[0]
    val_999999 = row_999999["Original Phone"]
    print(f"Extracted 'Original Phone' for payroll 999999: '{val_999999}'")
    assert val_999999 == "", f"Expected empty string but got {val_999999}"

    # Verify that Employee ID in the output has NO decimal point
    for id_val in mobile_df["Employee ID"]:
        assert "." not in str(id_val), f"Expected no decimal in Employee ID '{id_val}'"

    print("TEST PASSED!")

async def test_update_phone_endpoint():
    print("\nTesting update-phone API endpoint...")
    from apps.employee_phone_county_audit.views import update_phone, UpdatePhoneRequest
    from fastapi import Request
    from unittest.mock import Mock
    
    # Create a mock Request object
    mock_request = Mock(spec=Request)
    # Mock request.session.get("user") to return a mock user dictionary
    mock_request.session = {"user": {"id": 1, "email": "test@example.com"}}
    
    # Let's verify updating employee '112492' (Brittany Griffin, original employee_id 46648)
    from apps.position_requests.scheduler import _engine
    from sqlalchemy import text
    
    engine = _engine()
    # 1. Fetch current mobile number
    with engine.connect() as conn:
        current_data = conn.execute(text("SELECT mobile FROM employee WHERE employee_id = 46648")).fetchone()
        assert current_data is not None, "Employee 46648 not found in database!"
        original_mobile_in_db = current_data[0]
        
    print(f"Current mobile in DB for employee 46648: {original_mobile_in_db}")
    
    # Let's call the endpoint to toggle the number
    new_test_mobile = "727-426-4865" if original_mobile_in_db != "727-426-4865" else "172-742-6486"
    print(f"Attempting to update mobile to: {new_test_mobile}")
    
    payload = UpdatePhoneRequest(payroll_id="112492", original_phone=new_test_mobile)
    
    # Run the async update_phone function
    response = await update_phone(mock_request, payload)
    print(f"API Response: {response}")
    assert response["status"] == "success"
    
    # 3. Verify it updated in DB (using a new connection context)
    with engine.connect() as conn:
        updated_data = conn.execute(text("SELECT mobile FROM employee WHERE employee_id = 46648")).fetchone()
        print(f"Updated mobile in DB: {updated_data[0]}")
        assert updated_data[0] == new_test_mobile
        
        # Verify a history_entry was written
        history_data = conn.execute(text("SELECT changes, notes FROM history_entry WHERE related = 'Employee' AND related_id = 46648 ORDER BY created_at DESC LIMIT 1")).fetchone()
        print(f"History entry notes: {history_data[1]}")
        assert history_data[1] == "Phone Audit Fix"
        
    # 4. Clean up / Restore original mobile number in DB and delete log
    with engine.begin() as conn:
        conn.execute(text("UPDATE employee SET mobile = :original WHERE employee_id = 46648"), {"original": original_mobile_in_db})
        conn.execute(text("DELETE FROM history_entry WHERE related = 'Employee' AND related_id = 46648 AND notes = 'Phone Audit Fix'"))
        print("Restored database to original state successfully!")
        
    print("API ENDPOINT TEST PASSED!")

if __name__ == "__main__":
    test_audit_flow()
    import asyncio
    asyncio.run(test_update_phone_endpoint())

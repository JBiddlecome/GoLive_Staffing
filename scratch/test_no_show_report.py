from fastapi.testclient import TestClient
import sys
import os
from pathlib import Path

# Set working directory to project root
sys.path.insert(0, os.path.abspath('.'))

from app import app

# Remove RequireLoginMiddleware to bypass auth in tests
for middleware in app.user_middleware:
    if middleware.cls.__name__ == 'RequireLoginMiddleware':
        app.user_middleware.remove(middleware)
app.middleware_stack = app.build_middleware_stack()

client = TestClient(app)

def test_no_show_report():
    print("=== Start No Show Report Tests ===")

    # 1. Test HTML Dashboard page
    print("\n--- Test 1: GET /no-show-report ---")
    res = client.get("/no-show-report")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    html = res.text
    assert "No Show Report" in html, "Page title/header 'No Show Report' not found!"
    assert 'id="start_date"' in html, "Start date input is missing!"
    assert 'id="end_date"' in html, "End date input is missing!"
    assert 'id="pull_prev_week"' in html, "Button to pull previous Monday-Sunday is missing!"
    assert 'id="email_report"' in html, "Email Report button is missing!"
    print("OK: HTML page structure, date inputs, and action buttons are present.")

    # 2. Test JSON data endpoint
    print("\n--- Test 2: GET /no-show-report/data ---")
    start_date = "2020-01-01"
    end_date = "2030-01-01"
    res = client.get(f"/no-show-report/data?start_date={start_date}&end_date={end_date}")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    
    data_res = res.json()
    assert "data" in data_res, "JSON response missing 'data' list!"
    records = data_res["data"]
    print(f"OK: API returned successfully with {len(records)} records.")

    if len(records) > 0:
        print("Validating first record structure...")
        rec = records[0]
        expected_keys = [
            "timesheet_id", "employee_name", "event_date", "shift_start", 
            "shift_end", "client_name", "employee_worked", "client_worked", 
            "cancel_reason", "additional_notes"
        ]
        for key in expected_keys:
            assert key in rec, f"Key '{key}' is missing from record data!"
        print(f"OK: Record schema matches expectations. First record: {rec['employee_name']} @ {rec['client_name']}")
    else:
        print("Info: No 'No Show' timesheets exist in the test/current database state.")

    # 3. Test Email Report generation
    print("\n--- Test 3: POST /no-show-report/send-email (Manual trigger) ---")
    # Clean previous mock outputs if any
    for p in Path("scratch").glob("mock_no_show_email_*.html"):
        try:
            p.unlink()
        except OSError:
            pass

    res = client.post(f"/no-show-report/send-email?start_date={start_date}&end_date={end_date}")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    
    email_res = res.json()
    assert email_res.get("success") is True, f"Expected success=True, got {email_res}"
    print("OK: Email endpoint executed successfully.")
    
    # Verify mock file creation
    if email_res.get("mocked"):
        mock_files = list(Path("scratch").glob("mock_no_show_email_*.html"))
        assert len(mock_files) > 0, "No mock HTML email file was written to scratch directory!"
        print(f"OK: Verified mock HTML email file was generated: {mock_files[0]}")
    else:
        print(f"OK: Real email flow executed/attempted successfully. Info: {email_res.get('info')}")
    
    print("\nALL TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    try:
        test_no_show_report()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)

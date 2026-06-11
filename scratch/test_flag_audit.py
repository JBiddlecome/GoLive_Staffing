from fastapi.testclient import TestClient
import sys
import os

# Set working directory to project root so app can find templates
sys.path.insert(0, os.path.abspath('.'))

from app import app

# Remove RequireLoginMiddleware to bypass auth
for middleware in app.user_middleware:
    if middleware.cls.__name__ == 'RequireLoginMiddleware':
        app.user_middleware.remove(middleware)
app.middleware_stack = app.build_middleware_stack()

client = TestClient(app)

def test_flag_audit_page():
    print("Requesting /flag-audit...")
    # 1. Test page with default parameters (flags 0 and 1)
    res = client.get("/flag-audit")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    html = res.text
    
    print("\n--- Test 1: Defaults (Orange & Red) ---")
    assert 'value="2"' in html, "Green flag checkbox value='2' is missing!"
    print("OK: Green flag checkbox present.")
    assert 'value="none"' in html, "No flag checkbox value='none' is missing!"
    print("OK: No flag checkbox present.")
    assert 'Shifts (Last Year)' in html, "Shifts (Last Year) header is missing!"
    print("OK: Shifts (Last Year) header present.")
    assert 'Export Excel' in html, "Export Excel button is missing!"
    print("OK: Export Excel button present.")

    # 2. Test filtering by Green flag (flag=2)
    print("\n--- Test 2: Filter by Green Flag (value='2') ---")
    res_green = client.get("/flag-audit?flags=2")
    assert res_green.status_code == 200
    html_green = res_green.text
    if 'No employees found' in html_green:
        print("  Info: No active employees currently have Green Flag (value 2) in status (1, 3, 10, 14).")
    else:
        print("OK: Employees with Green flag returned successfully.")
        assert 'Green' in html_green

    # 3. Test filtering by No Flag (value='none')
    print("\n--- Test 3: Filter by No Flag (value='none') ---")
    res_none = client.get("/flag-audit?flags=none")
    assert res_none.status_code == 200
    html_none = res_none.text
    if 'No employees found' in html_none:
        print("  Info: No active employees found with No Flag.")
    else:
        print("OK: Employees with No Flag returned successfully.")
        assert 'No Flag' in html_none

    # 4. Test Excel Export
    print("\n--- Test 4: Excel Export ---")
    res_export = client.get("/flag-audit/export?flags=0&flags=1")
    assert res_export.status_code == 200, f"Expected 200, got {res_export.status_code}"
    assert "spreadsheetml.sheet" in res_export.headers.get("content-type", ""), f"Expected Excel content-type, got {res_export.headers.get('content-type')}"
    print("OK: Content-type header matches Excel.")
    
    # Load returned content with pandas
    import pandas as pd
    import io
    df = pd.read_excel(io.BytesIO(res_export.content))
    expected_cols = [
        "Employee ID",
        "Employee Name",
        "Flag Color",
        "DNR (Last 2 Years)",
        "Disciplinary Action (Last 2 Years)",
        "Shifts (Last Year)"
    ]
    for col in expected_cols:
        assert col in df.columns, f"Column '{col}' is missing from the exported Excel file!"
    print("OK: Exported Excel contains all expected columns.")
    print(f"OK: Exported Excel has {len(df)} rows.")

    print("\nALL TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    try:
        test_flag_audit_page()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)

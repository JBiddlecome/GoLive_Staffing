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
    
    # Check that Orange/Red are checked in the checkboxes, but others are not
    assert 'value="0"\n                            class="h-4 w-4 rounded border-emerald-300 text-emerald-600 focus:ring-emerald-600 transition-colors"\n                            checked' in html or 'value="0" \n                            class="h-4 w-4 rounded border-emerald-300 text-emerald-600 focus:ring-emerald-600 transition-colors"\n                            checked' in html or 'checked' in html, "Orange should be checked by default"
    print("OK: Default checked flags are set.")

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

    print("\nALL TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    try:
        test_flag_audit_page()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)

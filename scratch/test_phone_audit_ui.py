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
    # Required columns: "Status", "Employee ID", "Mobile", "County of Residence", "First Name", "Last Name", "Email"
    data = {
        "Status": ["Active"],
        "Employee ID": [46648],
        "Mobile": ["172-742-6486"],  # Begins with 1
        "County of Residence": ["Orange"],
        "First Name": ["John"],
        "Last Name": ["Doe"],
        "Email": ["john.doe@example.com"]
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
    print(mobile_df.to_dict(orient="records"))
    
    # Check that "Original Phone" column exists and has the expected value
    assert "Original Phone" in mobile_df.columns, "'Original Phone' column missing!"
    val = mobile_df.iloc[0]["Original Phone"]
    print(f"Extracted 'Original Phone' value: {val}")
    assert val == "727-426-4865", f"Expected '727-426-4865' but got {val}"
    print("TEST PASSED!")

if __name__ == "__main__":
    test_audit_flow()

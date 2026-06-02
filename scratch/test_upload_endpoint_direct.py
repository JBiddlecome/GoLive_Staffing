import asyncio
import os
import io
import sys
from fastapi import UploadFile

# Add root folder to sys.path
sys.path.append(r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing")
from apps.pay_rate_reduction_calculator.views import upload_spreadsheet

async def test_direct_upload():
    csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\apps\pay_rate_reduction_calculator\Markup_Analysis_Option_A.csv"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return
        
    print(f"Reading file contents from {csv_path}...")
    with open(csv_path, 'rb') as f:
        file_bytes = f.read()
        
    file_like = io.BytesIO(file_bytes)
    # Create the FastAPI UploadFile mock
    mock_file = UploadFile(
        filename="Markup_Analysis_Option_A.csv",
        file=file_like
    )
    
    print("Calling upload_spreadsheet directly...")
    try:
        response = await upload_spreadsheet(file=mock_file)
        print("Response received successfully:")
        print(response)
        
        # Verify that uploaded_rates.csv exists in the target folder
        std_csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\tmp\pay_rate_reduction_calculator\uploaded_rates.csv"
        if os.path.exists(std_csv_path):
            print(f"[OK] Standardized file successfully written to: {std_csv_path}")
            df = __import__('pandas').read_csv(std_csv_path)
            print(f"[OK] Verified uploaded file has {len(df)} rows.")
            print(df.head(5))
        else:
            print("[FAIL] Error: Standardized file was not written!")
    except Exception as e:
        print("[FAIL] Direct function call failed with exception:", e)

if __name__ == "__main__":
    asyncio.run(test_direct_upload())

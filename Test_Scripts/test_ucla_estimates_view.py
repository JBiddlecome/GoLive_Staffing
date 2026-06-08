import os
import asyncio
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.ucla_hours_tool.views import get_ucla_estimates

async def test_view():
    print("Testing UCLA Estimates route...")
    try:
        response = await get_ucla_estimates(start_date="2026-05-14", end_date="2026-05-14")
        print("Success! Endpoint returned:")
        print("Response type:", type(response))
        print("Number of records:", len(response.get("data", [])))
        if len(response.get("data", [])) > 0:
            first = response["data"][0]
            print("First record:", first)
            
            # Verify required columns are present
            required_keys = ['date', 'client', 'venue', 'position', 'start_time', 'end_time', 'employee', 'hours', 'bill_rate', 'amount', 'filled']
            missing_keys = [k for k in required_keys if k not in first]
            if not missing_keys:
                print("Verification Passed: All required fields present.")
            else:
                print("Verification Failed: Missing fields:", missing_keys)
        else:
            print("No records found for test range.")
    except Exception as e:
        print("Error executing route:", e)

if __name__ == "__main__":
    asyncio.run(test_view())

import os
import sys
import asyncio
import pandas as pd
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

sys.path.insert(0, r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing")
import apps.profit_tracker.views as views

ProfitPayload = views.ProfitPayload
payload = ProfitPayload(start_date="2026-04-13", end_date="2026-04-19", clients=["1785"])

async def main():
    try:
        response = await views.get_profit_data(payload)
        
        content = response.body
        import json
        data = json.loads(content)
        
        print(f"=== DMC Staff Inc. (Client 1785) ===")
        print(f"Total Gross Pay: {data['gross_pay']}")
        print(f"Total Bill:      {data['total_bill']}")
        
        print(f"\n--- Client Breakdown ---")
        for c in data.get("client_breakdown", []):
            print(f"  {c['client_name']}: GP={c['gross_pay']}, Bill={c['total_bill']}")
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())

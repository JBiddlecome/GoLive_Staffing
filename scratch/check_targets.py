import sys
sys.path.insert(0, r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing')
import os
import asyncio
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

# Import and call profit_data directly
from apps.profit_tracker.views import get_profit_data, ProfitPayload

class MockPayload:
    start_date = "2026-04-13"
    end_date = "2026-04-19"
    other_work = []

async def main():
    result = await get_profit_data(MockPayload())
    # Find specific clients
    target_clients = {"DMC Staff Inc.", "Vibiana Events and Redbird Events"}
    for item in result.get("client_breakdown", []):
        if item["client_name"] in target_clients:
            print(f"{item['client_name']}:")
            print(f"  GP={item['gross_pay']}, Bill={item['total_bill']}")

asyncio.run(main())

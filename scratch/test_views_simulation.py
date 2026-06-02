import os
import asyncio
from pydantic import BaseModel
from typing import Dict, Optional
from dotenv import load_dotenv

# Set up environment path
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

# Let's import our router endpoint and request payload
import sys
sys.path.append(r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing")
from apps.pay_rate_reduction_calculator.views import CompassCalculateRequest, calculate_compass_rates

async def main():
    print("Testing Global Reduction Mode...")
    payload_global = CompassCalculateRequest(
        start_date="2026-04-25",
        end_date="2026-05-24",
        reduction_amount=2.0,
        markup_percent=77.0,
        msp_filter="2",
        use_spreadsheet=False
    )
    
    result_global = await calculate_compass_rates(payload_global)
    if isinstance(result_global, dict):
        print("Global Reduction Results:")
        print(f"  Baseline Profit: ${result_global['baseline']['profit']:.2f}")
        print(f"  Simulated Profit: ${result_global['simulated']['profit']:.2f}")
        print(f"  Profit Net Change: ${result_global['simulated']['profit'] - result_global['baseline']['profit']:.2f}")
    else:
        print("Error in global simulation:", result_global)
        
    print("\nTesting Spreadsheet Mode (Fallback/Default Seeded)...")
    payload_spreadsheet = CompassCalculateRequest(
        start_date="2026-04-25",
        end_date="2026-05-24",
        reduction_amount=2.0,
        markup_percent=77.0,
        msp_filter="2",
        use_spreadsheet=True
    )
    
    result_spreadsheet = await calculate_compass_rates(payload_spreadsheet)
    if isinstance(result_spreadsheet, dict):
        print("Spreadsheet Results:")
        print(f"  Baseline Profit: ${result_spreadsheet['baseline']['profit']:.2f}")
        print(f"  Simulated Profit: ${result_spreadsheet['simulated']['profit']:.2f}")
        print(f"  Profit Net Change: ${result_spreadsheet['simulated']['profit'] - result_spreadsheet['baseline']['profit']:.2f}")
        
        # Look at the client breakdown list
        breakdown = result_spreadsheet.get('client_breakdown', [])
        print("\nTop 5 Client Breakdown (Spreadsheet Mode):")
        for client in breakdown[:5]:
            print(f"  Client: {client['client_name']}")
            print(f"    Orig Bill: ${client['orig_bill']:.2f}, Sim Bill: ${client['sim_bill']:.2f}")
            print(f"    Orig Pay: ${client['orig_pay']:.2f}, Sim Pay: ${client['sim_pay']:.2f}")
            print(f"    Orig Profit: ${client['orig_profit']:.2f}, Sim Profit: ${client['sim_profit']:.2f}")
            print(f"    Profit Delta: ${client['profit_change']:.2f}")
    else:
        print("Error in spreadsheet simulation:", result_spreadsheet)

if __name__ == "__main__":
    asyncio.run(main())

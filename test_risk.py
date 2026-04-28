from dotenv import load_dotenv
import asyncio

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

from apps.shift_risk_dashboard.views import get_risk_data, _engine, MAIN_SQL

async def main():
    print("Testing DB connection...")
    try:
        engine = _engine()
        with engine.connect() as conn:
            print("Connected.")
            print("Executing SQL...")
            result = conn.execute(MAIN_SQL)
            rows = result.fetchall()
            print(f"Got {len(rows)} rows.")
            
        print("Testing get_risk_data endpoint...")
        response = await get_risk_data()
        print(f"Response status: {response.status_code}")
        with open('test_data.json', 'w') as f:
            f.write(response.body.decode('utf-8'))
        print("Wrote response to test_data.json")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())

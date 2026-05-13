import asyncio
import os
import sys

# Add current dir to path to find apps
sys.path.append(os.getcwd())

async def test_logic():
    from apps.client_notifications.views import get_prior_month_data, _engine
    from sqlalchemy import text
    
    engine = _engine()
    try:
        # Get one client id
        with engine.begin() as conn:
            cid = conn.execute(text("SELECT client_id FROM client LIMIT 1")).scalar()
            print(f"Testing for client_id: {cid}")
            
        data = await get_prior_month_data(cid)
        print("Data gathered successfully")
        print(data)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_logic())

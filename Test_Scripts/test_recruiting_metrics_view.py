import os
import asyncio
from starlette.requests import Request
from dotenv import load_dotenv

# Load correct environment variables before importing view
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.recruiting_metrics.views import page

async def test_view():
    print("Testing Recruiting Metrics page loading...")
    # Construct a minimal Starlette request
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/recruiting-metrics",
        "headers": [],
        "query_string": b"",
    }
    req = Request(scope)
    
    try:
        response = await page(req)
        print("Success! Page rendered.")
        print("Response type:", type(response))
        html_content = response.body.decode("utf-8")
        print("HTML Snippet:")
        print(html_content[:500])
        
        # Check if 'Live Database' is present in HTML
        if "Live Database" in html_content:
            print("\nVerification Passed: 'Live Database' data source displayed correctly in HTML.")
        else:
            print("\nWarning: 'Live Database' not found in HTML. Check template rendering.")
    except Exception as e:
        print("Error rendering page:", e)

if __name__ == "__main__":
    asyncio.run(test_view())

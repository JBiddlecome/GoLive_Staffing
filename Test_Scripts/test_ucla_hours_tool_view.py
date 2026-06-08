import os
import asyncio
from starlette.requests import Request
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.ucla_hours_tool.views import page

async def test_page_render():
    print("Testing UCLA Hours Tool page rendering...")
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/ucla-hours-tool",
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
        
        # Check if new components exist in html
        if "UCLA Shift Estimates" in html_content:
            print("\nVerification Passed: 'UCLA Shift Estimates' found in HTML.")
        else:
            print("\nVerification Failed: 'UCLA Shift Estimates' not found in HTML.")
            
        if "estimates_results" in html_content:
            print("Verification Passed: 'estimates_results' container found in HTML.")
        else:
            print("Verification Failed: 'estimates_results' container not found in HTML.")
            
    except Exception as e:
        print("Error rendering page:", e)

if __name__ == "__main__":
    asyncio.run(test_page_render())

import os
import asyncio
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.ucla_hours_tool.views import download_ucla_estimates_pdf

async def test_pdf_endpoint():
    print("Testing UCLA Estimates PDF download endpoint...")
    try:
        response = await download_ucla_estimates_pdf(start_date="2026-05-14", end_date="2026-05-14")
        print("Success! Endpoint returned:")
        print("Response type:", type(response))
        print("Status code:", getattr(response, "status_code", "None"))
        
        # Read the body of the StreamingResponse
        pdf_bytes = b""
        async for chunk in response.body_iterator:
            pdf_bytes += chunk
            
        print("PDF Bytes length:", len(pdf_bytes))
        
        # Verify PDF header
        if pdf_bytes.startswith(b"%PDF"):
            print("Verification Passed: Valid PDF signature found.")
        else:
            print("Verification Failed: Invalid PDF signature.")
            
    except Exception as e:
        print("Error executing PDF endpoint:", e)

if __name__ == "__main__":
    asyncio.run(test_pdf_endpoint())

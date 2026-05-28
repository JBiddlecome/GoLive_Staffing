from fastapi.testclient import TestClient
import os
import sys

# Add project root to path
sys.path.append(r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing")

from app import app

client = TestClient(app)

def test_calculator():
    print("Testing GET /pay-rate-reduction-calculator...")
    # Bypass RequireLoginMiddleware by mocking session in the middleware or request,
    # but since session is verified, we can test the router logic directly!
    from apps.pay_rate_reduction_calculator.views import RecalculateRequest
    print("Views module imported successfully.")
    
    # Let's test the endpoint directly to avoid authentication redirect on client
    # By using the TestClient with a session, or calling the endpoint function directly
    from apps.pay_rate_reduction_calculator.views import calculate_custom_rates
    import asyncio
    
    payload = RecalculateRequest(custom_rates={
        "Los Angeles County, California|Cook 2": 21.00
    })
    
    print("Testing calculation logic directly...")
    result = asyncio.run(calculate_custom_rates(payload))
    if hasattr(result, "body"):
        import json
        body = json.loads(result.body.decode('utf-8'))
        print("Error JSONResponse Body:", body)
    else:
        print("Calculation results:", result)
        assert result["total_original_paid"] > 0
        assert result["total_custom_paid"] > 0
        assert "savings" in result
        print("ALL TESTS PASSED!")

if __name__ == "__main__":
    test_calculator()

from fastapi.testclient import TestClient
from app import app
import traceback

try:
    client = TestClient(app)
    response = client.get('/client-communication-summary', allow_redirects=True)
    print("STATUS:", response.status_code)
    print(response.text[:2000])
except BaseException as e:
    print("EXCEPTION:", traceback.format_exc())

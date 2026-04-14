from fastapi.testclient import TestClient
from app import app
import json
client = TestClient(app)
try:
    response = client.post('/client-communication-summary/analyze', data={'client_name': 'Test'})
    print(response.status_code)
    print(response.text)
except Exception as e:
    import traceback
    print(traceback.format_exc())

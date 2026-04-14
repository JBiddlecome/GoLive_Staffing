from fastapi.testclient import TestClient
from app import app
import traceback
import sys

client = TestClient(app)
# Inject mock session for RequireLoginMiddleware
# It requires request.session.get("user") so we set a session cookie or we mock the middleware
# Wait, actually, let's just monkeypatch the middleware check:
from starlette.middleware.base import BaseHTTPMiddleware
for middleware in app.user_middleware:
    if middleware.cls.__name__ == 'RequireLoginMiddleware':
        app.user_middleware.remove(middleware)

# Re-build app middleware stack since we changed it:
app.middleware_stack = app.build_middleware_stack()

try:
    response = client.get('/client-communication-summary', allow_redirects=True)
    print("STATUS:", response.status_code)
    print("TEXT:", response.text[:1000])
except Exception as e:
    print("EXCEPTION:", traceback.format_exc())

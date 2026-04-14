import traceback
import sys
from fastapi.testclient import TestClient
from app import app, RequireLoginMiddleware

# Remove RequireLoginMiddleware
for i, m in enumerate(app.user_middleware):
    if m.cls == RequireLoginMiddleware:
        del app.user_middleware[i]
app.middleware_stack = app.build_middleware_stack()

client = TestClient(app)
try:
    response = client.get('/client-communication-summary')
    print("STATUS:", response.status_code)
    if response.status_code == 500:
        print("500 ERROR CAUSE:")
        print(response.text)
except BaseException as e:
    print(traceback.format_exc())

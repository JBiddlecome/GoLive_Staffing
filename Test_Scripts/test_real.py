import uvicorn
import threading
import time
import requests
import traceback
from app import app
from starlette.middleware.base import BaseHTTPMiddleware

# remove auth
for i, m in enumerate(app.user_middleware):
    if m.cls.__name__ == 'RequireLoginMiddleware':
        del app.user_middleware[i]
        break
app.middleware_stack = app.build_middleware_stack()

def run_server():
    uvicorn.run(app, host='127.0.0.1', port=8001, log_level='error')

t = threading.Thread(target=run_server, daemon=True)
t.start()
time.sleep(4)

try:
    r = requests.get('http://127.0.0.1:8001/client-communication-summary')
    print("STATUS:", r.status_code)
    print(r.text)
except Exception as e:
    print(traceback.format_exc())

import uvicorn
import threading
import time
import requests
from app import app

def run_server():
    uvicorn.run(app, host='127.0.0.1', port=8001, log_level='error')

t = threading.Thread(target=run_server, daemon=True)
t.start()
time.sleep(3)

print('GETTING')
# Authenticate
client = requests.Session()
# Bypassing by hitting login might not work if we need a token.
# Let's hit the URL, and if we get 500 we will see it!
r = client.get('http://127.0.0.1:8001/client-communication-summary')
print(r.status_code)
print(r.text[:500])


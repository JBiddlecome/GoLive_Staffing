import os
from dotenv import load_dotenv
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

import uvicorn
from app import app

if __name__ == "__main__":
    print("Starting server in foreground...")
    uvicorn.run(app, host='127.0.0.1', port=8001)

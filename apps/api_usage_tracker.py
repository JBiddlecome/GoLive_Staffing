import sqlite3
import os
from pathlib import Path
from datetime import datetime
from fastapi import Request

def _resolve_data_dir() -> Path:
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/opt/render/project/src/data")
    return Path(__file__).resolve().parents[1] / "data"

DATA_DIR = _resolve_data_dir()

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = DATA_DIR / "api_usage_logs.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            app_name TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Initialize DB on import
init_db()

def log_api_usage(app_name: str, request: Request | None = None, user_email: str | None = None):
    """Log an API call for a specific app and user."""
    try:
        if not user_email:
            if request:
                user = request.session.get("user")
                if user and isinstance(user, dict):
                    user_email = user.get("email") or user.get("name") or str(user)
                elif user:
                    user_email = str(user)
                else:
                    user_email = "Unauthenticated User"
            else:
                user_email = "System Background Task"
                
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO api_usage (user_email, app_name) VALUES (?, ?)',
            (user_email, app_name)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to log API usage for {app_name}: {e}")

def get_api_usage_summary():
    """Get a summary of API usage grouped by user and app."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_email, app_name, COUNT(*) as count, MAX(timestamp) as last_used
            FROM api_usage
            GROUP BY user_email, app_name
            ORDER BY count DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Failed to retrieve API usage summary: {e}")
        return []

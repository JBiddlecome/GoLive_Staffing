import os
import logging
from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import bcrypt
from typing import Optional

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None, next: str = "/work-in-progress"):
    return templates.TemplateResponse("auth/login.html", {"request": request, "error": error, "next": next})

@router.post("/login")
async def do_login(request: Request, email: str = Form(...), password: str = Form(...), next: str = Form("/work-in-progress")):
    if not (email.endswith("@culinarystaffing.com") or email.endswith("@golivestaffing.com")):
        return templates.TemplateResponse("auth/login.html", {"request": request, "error": "Invalid email domain. Must be @culinarystaffing.com or @golivestaffing.com", "next": next})

    engine = _engine()
    try:
        with engine.connect() as conn:
            # We will perform a simple check for deleted_at if it exists, but to avoid crashes if it doesn't,
            # we can just select based on status=10.
            query = text("SELECT id, email, password_hash FROM user WHERE email = :email AND status = 10")
            user_row = conn.execute(query, {"email": email}).fetchone()
            
            if user_row:
                stored_hash = user_row[2] # password_hash
                
                # Raw bcrypt accepts $2b$ natively. The GoLive DB has $2y$ (PHP specific version).
                if stored_hash.startswith("$2y$"):
                    stored_hash = "$2b$" + stored_hash[4:]
                    
                # Use raw bcrypt.checkpw instead of passlib to avoid compatibility bugs with bcrypt 4+
                if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
                    # Login successful
                    request.session["user"] = {
                        "id": user_row[0],
                        "email": user_row[1],
                        "first_name": "User",
                        "last_name": ""
                    }
                    return RedirectResponse(url=next, status_code=303)
                else:
                    return templates.TemplateResponse("auth/login.html", {"request": request, "error": "Invalid email or password.", "next": next})
            else:
                return templates.TemplateResponse("auth/login.html", {"request": request, "error": "User not found or inactive.", "next": next})
    except Exception as e:
        logger.exception("Login error")
        return templates.TemplateResponse("auth/login.html", {"request": request, "error": f"An error occurred: {str(e)}", "next": next})
    finally:
        engine.dispose()

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=303, detail="Not authenticated", headers={"Location": "/auth/login?next=" + request.url.path})
    return user

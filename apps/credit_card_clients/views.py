from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

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


@router.get("", response_class=HTMLResponse)
async def credit_card_clients_page(request: Request):
    return templates.TemplateResponse(
        "apps/credit_card_clients.html",
        {"request": request},
    )


@router.get("/data")
async def get_credit_card_cancellations():
    """Return client-cancelled shifts for credit-card clients in the last 15 minutes."""
    engine = _engine()
    try:
        sql = text("""
            SELECT
                c.name            AS client_name,
                p.description     AS position,
                se.bill_rate      AS bill_rate,
                s.start           AS shift_date,
                se.deleted_at     AS deleted_at
            FROM shift_employee se
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s           ON sp.shift_id          = s.shift_id
            JOIN position p        ON sp.position_id       = p.position_id
            JOIN event ev          ON se.event_id          = ev.event_id
            JOIN client c          ON ev.client_id         = c.client_id
            WHERE se.cancel_reason = 4
              AND c.payment_type   = 1
              AND se.deleted_at   >= :fifteen_mins_ago
            ORDER BY se.deleted_at DESC;
        """)

        fifteen_mins_ago = (datetime.now() - timedelta(minutes=15)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with engine.begin() as connection:
            result = connection.execute(
                sql, {"fifteen_mins_ago": fifteen_mins_ago}
            ).mappings().all()

            data = []
            for row in result:
                item = dict(row)
                if item["shift_date"]:
                    item["shift_date"] = item["shift_date"].strftime("%Y-%m-%d %H:%M")
                if item["deleted_at"]:
                    item["deleted_at"] = item["deleted_at"].strftime(
                        "%Y-%m-%d %H:%M"
                    )
                item["bill_rate"] = (
                    float(item["bill_rate"]) if item["bill_rate"] else 0.0
                )

                data.append(
                    {
                        "client_name": item["client_name"],
                        "position": item["position"],
                        "bill_rate": item["bill_rate"],
                        "shift_date": item["shift_date"],
                        "deleted_at": item["deleted_at"],
                    }
                )

            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()


@router.get("/log")
async def get_credit_card_clients_log():
    """Diagnostic log: latest 50 credit-card client cancellations regardless of time window."""
    engine = _engine()
    try:
        with engine.begin() as connection:
            db_now = connection.execute(text("SELECT NOW()")).scalar()

            sql_recent = text("""
                SELECT
                    c.name            AS client_name,
                    p.description     AS position,
                    se.bill_rate      AS bill_rate,
                    s.start           AS shift_date,
                    se.deleted_at     AS deleted_at,
                    se.cancel_reason  AS cancel_reason,
                    c.payment_type    AS payment_type
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s           ON sp.shift_id          = s.shift_id
                JOIN position p        ON sp.position_id       = p.position_id
                JOIN event ev          ON se.event_id          = ev.event_id
                JOIN client c          ON ev.client_id         = c.client_id
                WHERE se.cancel_reason = 4
                  AND c.payment_type   = 1
                ORDER BY se.deleted_at DESC
                LIMIT 50;
            """)
            result_recent = connection.execute(sql_recent).mappings().all()
            latest = [dict(r) for r in result_recent]
            for r in latest:
                if r["shift_date"]:
                    r["shift_date"] = r["shift_date"].strftime("%Y-%m-%d %H:%M")
                if r["deleted_at"]:
                    r["deleted_at"] = r["deleted_at"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                if r["bill_rate"] is not None:
                    r["bill_rate"] = float(r["bill_rate"])

            # Diagnostic: ALL cancel_reason=4 shifts deleted today,
            # regardless of payment_type, to help identify filtering issues.
            sql_diag = text("""
                SELECT
                    se.shift_employee_id,
                    c.name            AS client_name,
                    c.payment_type    AS payment_type,
                    p.description     AS position,
                    se.bill_rate      AS bill_rate,
                    s.start           AS shift_date,
                    se.deleted_at     AS deleted_at,
                    se.cancelled_at   AS cancelled_at
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s           ON sp.shift_id          = s.shift_id
                JOIN position p        ON sp.position_id       = p.position_id
                JOIN event ev          ON se.event_id          = ev.event_id
                JOIN client c          ON ev.client_id         = c.client_id
                WHERE se.cancel_reason = 4
                  AND se.deleted_at   >= CURDATE()
                ORDER BY se.deleted_at DESC
                LIMIT 50;
            """)
            result_diag = connection.execute(sql_diag).mappings().all()
            diag = [dict(r) for r in result_diag]
            for r in diag:
                if r["shift_date"]:
                    r["shift_date"] = r["shift_date"].strftime("%Y-%m-%d %H:%M")
                if r["deleted_at"]:
                    r["deleted_at"] = r["deleted_at"].strftime("%Y-%m-%d %H:%M:%S")
                if r["cancelled_at"]:
                    r["cancelled_at"] = r["cancelled_at"].strftime("%Y-%m-%d %H:%M:%S")
                if r["bill_rate"] is not None:
                    r["bill_rate"] = float(r["bill_rate"])

            log_data = {
                "database_time": (
                    db_now.strftime("%Y-%m-%d %H:%M:%S") if db_now else None
                ),
                "latest_cc_cancellations": latest,
                "diagnostic_all_cancel_reason_4_today": diag,
            }
            return JSONResponse(log_data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

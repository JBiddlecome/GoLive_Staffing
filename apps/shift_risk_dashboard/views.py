from __future__ import annotations
import os
import time
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ─── Simple in-process cache (2-minute TTL) ────────────────────────────────
_cache: dict[str, Any] = {}
_CACHE_TTL_SECONDS = 120


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    missing = [
        env_name
        for env_name, value in (("DB_HOST", host), ("DB_USER", user), ("DB_PASSWORD", password))
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required database environment variables: {', '.join(missing)}",
        )

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


# ─── Risk Algorithm ────────────────────────────────────────────────────────

def _calculate_risk_score(open_spots: int, total_spots: int, remaining_hours: float, projected_fill: float) -> float:
    if total_spots <= 0:
        return 0.0

    unfilled_pct = open_spots / total_spots

    if remaining_hours <= 0:
        time_urgency = 1.0
    elif remaining_hours <= 24:
        time_urgency = 0.9
    elif remaining_hours <= 48:
        time_urgency = 0.75
    elif remaining_hours <= 72:
        time_urgency = 0.55
    elif remaining_hours <= 120:
        time_urgency = 0.35
    else:
        time_urgency = 0.15

    if projected_fill <= 0:
        trajectory_deficit = 1.0
    else:
        shortfall = max(0, total_spots - projected_fill)
        trajectory_deficit = min(1.0, shortfall / total_spots)

    risk_score = (unfilled_pct * 0.40) + (time_urgency * 0.40) + (trajectory_deficit * 0.20)
    return round(risk_score * 100, 1)


def _risk_level(remaining_hours: float, is_past_deadline: bool) -> str:
    if is_past_deadline or remaining_hours <= 24:
        return "CRITICAL"
    elif remaining_hours <= 72:
        return "WARNING"
    else:
        return "AT RISK"


def _process_rows(rows) -> dict:
    """
    Takes raw SQL rows and applies the risk algorithm in Python.
    Returns the full JSON-ready payload.
    """
    now = datetime.utcnow()
    today = now.date()

    # Group rows by event
    events_map: dict[int, dict] = {}

    def _parse_dt(val: Any) -> datetime | None:
        if not val:
            return None
        if isinstance(val, datetime):
            return val
        if isinstance(val, str):
            val = val.replace(" ", "T")
            try:
                return datetime.fromisoformat(val)
            except ValueError:
                pass
        return None

    def _safe_float(val: Any) -> float | None:
        try:
            return float(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    for row in rows:
        event_id = row["event_id"]
        shift_id = row["shift_id"]
        shift_position_id = row["shift_position_id"]

        # ── Time calculations ────────────────────────────────────────────
        event_date_val = row["event_date"]
        if isinstance(event_date_val, str):
            try:
                event_date = datetime.strptime(event_date_val.split(" ")[0].split("T")[0], "%Y-%m-%d").date()
            except ValueError:
                event_date = today
        elif isinstance(event_date_val, datetime):
            event_date = event_date_val.date()
        else:
            event_date = event_date_val

        shift_start = _parse_dt(row["shift_start"]) or now

        # T0: earliest publish time, or fall back to position created_at
        t0 = _parse_dt(row["first_published_at"]) or _parse_dt(row["position_created_at"]) or now

        # T_deadline = shift.start - 24 hours (edge: event is today → T_deadline = shift.start)
        if event_date == today:
            t_deadline = shift_start
        else:
            t_deadline = shift_start - timedelta(hours=24)

        is_past_deadline = now >= t_deadline
        remaining_hours = (t_deadline - now).total_seconds() / 3600
        hours_until_shift = (shift_start - now).total_seconds() / 3600

        total_window_hours = max(0.0, (t_deadline - t0).total_seconds() / 3600)
        elapsed_hours = max(0.0, (now - t0).total_seconds() / 3600)

        try:
            total_spots = int(row["total_spots"])
        except (ValueError, TypeError):
            total_spots = 0

        if total_spots <= 0:
            continue

        try:
            filled_spots = int(row["filled_spots"])
        except (ValueError, TypeError):
            filled_spots = 0

        open_spots = total_spots - filled_spots
        if open_spots <= 0:
            continue

        # Fill rate & projection
        if elapsed_hours >= 1:
            fill_rate_per_hour = filled_spots / elapsed_hours
            projected_fill = fill_rate_per_hour * total_window_hours if total_window_hours > 0 else filled_spots
        else:
            fill_rate_per_hour = 0.0
            projected_fill = float(filled_spots)

        fill_pct = round((filled_spots / total_spots) * 100, 1)

        # ── Risk determination ───────────────────────────────────────────
        risk_score = _calculate_risk_score(open_spots, total_spots, remaining_hours, projected_fill)
        level = _risk_level(remaining_hours, is_past_deadline)

        published_at = _parse_dt(row["first_published_at"])
        published_at_str = (published_at.isoformat() + "Z") if published_at else None

        position_entry = {
            "shift_position_id": shift_position_id,
            "position_name": row["position_name"],
            "total_spots": total_spots,
            "filled_spots": filled_spots,
            "open_spots": open_spots,
            "pay_rate": _safe_float(row["pay_rate"]),
            "bill_rate": _safe_float(row["bill_rate"]),
            "fill_pct": fill_pct,
            "fill_rate_per_hour": round(fill_rate_per_hour, 4),
            "projected_fill": round(projected_fill, 2),
            "hours_until_deadline": round(remaining_hours, 2),
            "hours_until_shift": round(hours_until_shift, 2),
            "published_at": published_at_str,
            "t0": t0.isoformat() + "Z",           # actual T0 used in calculations (never null)
            "t_deadline": t_deadline.isoformat() + "Z",  # 24h-before-shift deadline
            "risk_score": risk_score,
            "risk_level": level,
            "is_past_deadline": is_past_deadline,
            "timeline": [],  # populated on demand via separate endpoint
        }

        # ── Build events → shifts → positions hierarchy ──────────────────
        if event_id not in events_map:
            days_until = (event_date - today).days
            events_map[event_id] = {
                "event_id": event_id,
                "event_date": str(event_date),
                "event_title": row["event_title"],
                "client_name": row["client_name"],
                "venue_name": row["venue_name"],
                "staffing_manager_id": row.get("staffing_manager_id"),
                "staffing_manager_name": row.get("staffing_manager_name"),
                "days_until_event": days_until,
                "total_open_spots": 0,
                "total_spots": 0,
                "max_risk_score": 0.0,
                "min_hours_until_shift": float("inf"),
                "shifts": {},
            }

        ev = events_map[event_id]
        ev["total_open_spots"] += open_spots
        ev["total_spots"] += total_spots
        ev["max_risk_score"] = max(ev["max_risk_score"], risk_score)
        ev["min_hours_until_shift"] = min(ev["min_hours_until_shift"], hours_until_shift)

        if shift_id not in ev["shifts"]:
            s_start = _parse_dt(row["shift_start"])
            s_end = _parse_dt(row["shift_end"])
            ev["shifts"][shift_id] = {
                "shift_id": shift_id,
                "shift_start": (s_start.isoformat() + "Z") if s_start else "",
                "shift_end": (s_end.isoformat() + "Z") if s_end else "",
                "positions": [],
            }

        ev["shifts"][shift_id]["positions"].append(position_entry)

    # ── Sort and flatten ─────────────────────────────────────────────────
    events_list = []
    for ev in sorted(events_map.values(), key=lambda e: (e["event_date"], -e["max_risk_score"])):
        shifts_list = []
        for sh in sorted(ev["shifts"].values(), key=lambda s: s["shift_start"]):
            sh["positions"].sort(key=lambda p: -p["risk_score"])
            shifts_list.append(sh)

        ev_out = {k: v for k, v in ev.items() if k != "shifts"}
        # Clamp float("inf") which is JSON-unsafe
        mh = ev_out.get("min_hours_until_shift", 9999.0)
        ev_out["min_hours_until_shift"] = round(min(mh, 9999.0), 2)
        ev_out["shifts"] = shifts_list
        events_list.append(ev_out)

    return events_list


# ─── SQL ────────────────────────────────────────────────────────────────────

MAIN_SQL = text("""
SELECT
    c.client_id,
    c.name                                          AS client_name,
    v.venue_id,
    v.name                                          AS venue_name,
    v.staffing_manager_id,
    CONCAT(u_mgr.first_name, ' ', u_mgr.last_name)  AS staffing_manager_name,
    e.event_id,
    e.title                                         AS event_title,
    DATE_FORMAT(e.date, '%Y-%m-%d')                 AS event_date,
    s.shift_id,
    s.start                                         AS shift_start,
    s.end                                           AS shift_end,
    sp.shift_position_id,
    p.description                                   AS position_name,
    sp.rate                                         AS pay_rate,
    sp.bill_rate,
    sp.count                                        AS total_spots,
    sp.was_published,
    sp.created_at                                   AS position_created_at,
    COALESCE(se_counts.filled, 0)                   AS filled_spots,
    (sp.count - COALESCE(se_counts.filled, 0))      AS open_spots,
    MIN(pub.created_at)                             AS first_published_at
FROM event e
JOIN client c       ON c.client_id   = e.client_id
JOIN venue v        ON v.venue_id    = e.venue_id
LEFT JOIN user u_mgr ON u_mgr.id = v.staffing_manager_id
JOIN shift s        ON s.event_id    = e.event_id
JOIN shift_position sp ON sp.shift_id = s.shift_id
JOIN position p     ON p.position_id = sp.position_id
LEFT JOIN publishing pub
    ON pub.event_id = e.event_id
    AND pub.deleted_at IS NULL
LEFT JOIN (
    SELECT shift_position_id, COUNT(shift_employee_id) AS filled
    FROM shift_employee
    WHERE cancel_reason = 0 AND deleted_at IS NULL
    GROUP BY shift_position_id
) se_counts ON se_counts.shift_position_id = sp.shift_position_id
WHERE e.date >= CURDATE()
  AND e.deleted_at IS NULL
  AND s.deleted_at IS NULL
  AND sp.deleted_at IS NULL
  AND sp.was_published != 0
  AND (sp.count - COALESCE(se_counts.filled, 0)) > 0
  AND sp.count > 0
GROUP BY
    c.client_id, v.venue_id, v.staffing_manager_id, u_mgr.first_name, u_mgr.last_name, e.event_id, s.shift_id,
    sp.shift_position_id, p.position_id
ORDER BY e.date ASC, s.start ASC
""")

TIMELINE_SQL = text("""
SELECT
    se.created_at       AS signup_timestamp,
    se.confirmed_at,
    se.shift_employee_id
FROM shift_employee se
WHERE se.shift_position_id = :shift_position_id
  AND se.cancel_reason = 0
  AND se.deleted_at IS NULL
ORDER BY se.created_at ASC
""")


# ─── Endpoints ──────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def shift_risk_dashboard_index(request: Request):
    return templates.TemplateResponse("apps/shift_risk_dashboard.html", {"request": request})


@router.get("/data")
async def get_risk_data():
    global _cache

    # Check cache
    cached = _cache.get("data")
    if cached and (time.monotonic() - cached["ts"]) < _CACHE_TTL_SECONDS:
        return JSONResponse(content=cached["payload"])

    engine = _engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(MAIN_SQL)
            rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        events_list = _process_rows(rows)

        # Summary counts
        critical = sum(1 for e in events_list if any(
            p["risk_level"] == "CRITICAL"
            for s in e["shifts"]
            for p in s["positions"]
        ))
        warning = sum(1 for e in events_list if any(
            p["risk_level"] == "WARNING"
            for s in e["shifts"]
            for p in s["positions"]
        ) and not any(
            p["risk_level"] == "CRITICAL"
            for s in e["shifts"]
            for p in s["positions"]
        ))
        at_risk_count = len(events_list) - critical - warning
        total_open = sum(e["total_open_spots"] for e in events_list)

        payload = {
            "generated_at": datetime.utcnow().isoformat(),
            "total_at_risk_events": len(events_list),
            "total_open_spots": total_open,
            "critical_events": critical,
            "warning_events": warning,
            "at_risk_events": at_risk_count,
            "events": events_list,
        }

        _cache["data"] = {"ts": time.monotonic(), "payload": payload}
        return JSONResponse(content=payload)

    except Exception as e:
        print(f"ERROR in get_risk_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()


@router.get("/summary")
async def get_risk_summary():
    """Lightweight counts for a status badge."""
    global _cache

    cached = _cache.get("data")
    if cached and (time.monotonic() - cached["ts"]) < _CACHE_TTL_SECONDS:
        p = cached["payload"]
        return JSONResponse(content={
            "total_at_risk_events": p["total_at_risk_events"],
            "critical_events": p["critical_events"],
            "warning_events": p["warning_events"],
            "at_risk_events": p["at_risk_events"],
            "total_open_spots": p["total_open_spots"],
        })

    # Re-use full data fetch
    data_response = await get_risk_data()
    # data_response is a JSONResponse — re-read via cache
    cached = _cache.get("data")
    p = cached["payload"]
    return JSONResponse(content={
        "total_at_risk_events": p["total_at_risk_events"],
        "critical_events": p["critical_events"],
        "warning_events": p["warning_events"],
        "at_risk_events": p["at_risk_events"],
        "total_open_spots": p["total_open_spots"],
    })


@router.get("/timeline/{shift_position_id}")
async def get_timeline(shift_position_id: int):
    """Returns cumulative fill timeline for a single shift position."""
    engine = _engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(TIMELINE_SQL, {"shift_position_id": shift_position_id})
            rows = result.fetchall()

        timeline = []
        cumulative = 0
        for row in rows:
            cumulative += 1
            ts = row[0]
            timeline.append({
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "cumulative_filled": cumulative,
            })

        return JSONResponse(content={"shift_position_id": shift_position_id, "timeline": timeline})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        engine.dispose()

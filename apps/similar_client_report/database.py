import math
import os
import time

import pandas as pd
import requests
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------------------------------------------------------------------------
# Industry enum values (must match client.industry column exactly)
# ---------------------------------------------------------------------------

INDUSTRY_CATEGORIES = [
    "CASINO",
    "CATERING_COMPANY",
    "CLUB",
    "CONVENTION",
    "CORPORATE_DINING",
    "ENTERTAINMENT_STUDIO",
    "HEALTHCARE",
    "HOTEL",
    "OTHER",
    "PRIVATE_EVENT",
    "PRODUCTION",
    "REHABILITATION",
    "RESTAURANT",
    "SCHOOL",
    "SENIOR_ASSISTED_LIVING",
    "STADIUM",
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

class _TTLCache:
    def __init__(self, ttl: int = 600):
        self._ttl = ttl
        self._data = None
        self._ts = 0.0

    def get(self):
        if self._data is not None and (time.time() - self._ts) < self._ttl:
            return self._data
        return None

    def set(self, data):
        self._data = data
        self._ts = time.time()


_msp_cache = _TTLCache(600)


def _db_url() -> URL:
    return URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.getenv("DB_NAME", "cstaffing_live"),
    )


def _engine():
    return create_engine(_db_url(), pool_pre_ping=True)


# ---------------------------------------------------------------------------
# MSP list
# ---------------------------------------------------------------------------

def fetch_msps() -> list[dict]:
    cached = _msp_cache.get()
    if cached is not None:
        return cached
    engine = _engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT id, name FROM msp ORDER BY name"), conn)
    engine.dispose()
    result = df.to_dict(orient="records")
    _msp_cache.set(result)
    return result


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def geocode_location(location_str: str) -> tuple[float | None, float | None]:
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": location_str, "format": "json", "limit": 1},
            headers={"User-Agent": "GoLiveStaffingTools/1.0 (internal)"},
            timeout=6,
        )
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception:
        pass
    return None, None


# ---------------------------------------------------------------------------
# Distance
# ---------------------------------------------------------------------------

def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3959.0
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Main query + scoring
# ---------------------------------------------------------------------------

_QUERY = text("""
    SELECT
        c.client_id,
        c.name            AS client_name,
        c.status          AS client_status,
        c.industry,
        c.industry_other,
        c.latitude,
        c.longitude,
        c.city,
        c.state,
        m.id              AS msp_id,
        m.name            AS msp_name,
        COALESCE(ss.shifts_last_year, 0) AS shifts_last_year,
        ss.last_shift_date
    FROM client c
    LEFT JOIN msp m ON c.msp_id = m.id
    LEFT JOIN (
        SELECT
            e.client_id,
            COUNT(DISTINCT CASE
                WHEN e.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) THEN t.timesheet_id
            END)            AS shifts_last_year,
            MAX(e.date)     AS last_shift_date
        FROM event e
        JOIN timesheet t ON t.event_id = e.event_id
        WHERE e.deleted_at IS NULL
        GROUP BY e.client_id
    ) ss ON c.client_id = ss.client_id
    WHERE c.deleted_at IS NULL
""")


def fetch_similar_clients(
    industries: list[str] | None,
    msp_id: str | None,
    client_name: str,
    lat: float | None,
    lon: float | None,
    weight_industry: float = 100.0,
    weight_msp: float = 50.0,
    weight_shifts: float = 1.0,
    weight_proximity: float = 1.0,
) -> list[dict]:
    selected_industries = {
        industry.strip()
        for industry in (industries or [])
        if industry and industry.strip()
    }

    engine = _engine()
    with engine.connect() as conn:
        df = pd.read_sql(_QUERY, conn)
    engine.dispose()

    if df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        client_industry = (row.get("industry") or "").strip()
        score = 0.0

        # 1. Industry — exact DB enum match (dominant weight)
        if selected_industries and client_industry in selected_industries:
            score += weight_industry

        # 2. MSP match
        if msp_id and str(row.get("msp_id") or "") == str(msp_id):
            score += weight_msp

        # 3. Shifts in last year (up to 100 pts, linear then capped)
        shifts = int(row.get("shifts_last_year") or 0)
        score += weight_shifts * min(shifts, 100)

        # 4. Location proximity (up to 10 pts)
        miles: float | None = None
        client_lat = row.get("latitude")
        client_lon = row.get("longitude")
        if lat is not None and lon is not None and client_lat and client_lon:
            try:
                miles = _haversine_miles(lat, lon, float(client_lat), float(client_lon))
                score += weight_proximity * max(0.0, 10.0 - miles / 10.0)
            except (ValueError, TypeError):
                miles = None

        last_shift = row.get("last_shift_date")
        last_shift_str = str(last_shift) if last_shift and str(last_shift) not in ("NaT", "None", "nan") else None

        status_map = {
            0: "Terminated",
            1: "Active",
            3: "Prospect",
            4: "Candidate Partner",
            10: "Inactive 60 days",
            11: "Inactive 180 days",
            12: "Inactive 365 days",
        }
        status_val = row.get("client_status")
        
        # Handle cases where status_val might be a string if pandas inferred object
        try:
            status_val = int(status_val) if pd.notna(status_val) else -1
        except (ValueError, TypeError):
            status_val = -1
            
        status_str = status_map.get(status_val, "Unknown")

        rows.append({
            "client_name": row.get("client_name"),
            "status": status_str,
            "industry": client_industry or (row.get("industry_other") or ""),
            "msp": row.get("msp_name") or "No MSP",
            "shifts_last_year": shifts,
            "last_shift_date": last_shift_str,
            "miles": round(miles, 1) if miles is not None else None,
            "city": row.get("city") or "",
            "state": row.get("state") or "",
            "score": round(score, 1),
        })

    rows.sort(key=lambda x: x["score"], reverse=True)
    return rows[:10]


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

async def get_msps_async() -> list[dict]:
    return await run_in_threadpool(fetch_msps)


async def get_similar_clients_async(
    industries: list[str] | None,
    msp_id: str | None,
    client_name: str,
    lat: float | None,
    lon: float | None,
    weight_industry: float = 100.0,
    weight_msp: float = 50.0,
    weight_shifts: float = 1.0,
    weight_proximity: float = 1.0,
) -> list[dict]:
    return await run_in_threadpool(
        fetch_similar_clients,
        industries,
        msp_id,
        client_name,
        lat,
        lon,
        weight_industry,
        weight_msp,
        weight_shifts,
        weight_proximity,
    )

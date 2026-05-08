import math
import os
import time

import pandas as pd
import requests
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------------------------------------------------------------------------
# Industry taxonomy (from business_taxonomy.md)
# ---------------------------------------------------------------------------

INDUSTRY_CATEGORIES = [
    "Corporate Dining / Workplace",
    "Education",
    "Healthcare / Senior Living",
    "Hospitality",
    "Events",
    "Food & Beverage",
    "Entertainment & Media",
    "Nonprofit / Social Services",
    "Residential / Facilities",
    "Retail / Commercial",
    "Arts / Culture / Recreation",
    "Religious / Institutional",
    "Staffing / Partners",
    "Private Clubs",
    "Other",
]

_TAXONOMY_KEYWORDS: dict[str, list[str]] = {
    "Corporate Dining / Workplace": ["corporate dining", "corporate", "workplace", "office", "workspace", "coworking", "management compan"],
    "Education": ["school", "university", "college", "education", "campus", "k-12", "k12", "academy"],
    "Healthcare / Senior Living": ["healthcare", "hospital", "senior", "assisted living", "rehabilitation", "rehab", "medical", "health care", "skilled nursing"],
    "Hospitality": ["hotel", "resort", "hospitality", "motel", "inn", "lodge"],
    "Events": ["event venue", "event coordinator", "event planner", "private event", "nightclub", "winery", "convention center", "banquet", "event space"],
    "Food & Beverage": ["restaurant", "cafe", "food truck", "catering", "ghost kitchen", "meal prep", "cafeteria", "dining", "bistro", "bakery", "bar "],
    "Entertainment & Media": ["stadium", "arena", "concession", "entertainment", "production", "media", "podcast", "marketing", "casino", "theatre", "theater", "music venue", "film"],
    "Nonprofit / Social Services": ["nonprofit", "non-profit", "social service", "job assistance", "career center", "apprenticeship", "community", "foundation", "charity"],
    "Residential / Facilities": ["apartment", "private residence", "senior center", "facilities management", "facility"],
    "Retail / Commercial": ["retail", "manufacturing", "commercial", "store", "shop", "warehouse"],
    "Arts / Culture / Recreation": ["museum", "art gallery", "camp", "farm", "garden", "recreation", "cultural", "zoo", "aquarium"],
    "Religious / Institutional": ["religious", "temple", "church", "mosque", "synagogue", "parish", "diocese"],
    "Staffing / Partners": ["staffing", "referral", "partner", "agency", "temp "],
    "Private Clubs": ["private club", "country club", "yacht club", "athletic club", "tennis club", "golf club", "club"],
}


def normalize_industry(raw: str) -> str:
    if not raw or not raw.strip():
        return "Other"
    s = raw.lower().strip()
    for category, keywords in _TAXONOMY_KEYWORDS.items():
        for kw in keywords:
            if kw in s:
                return category
    return raw.strip().title()


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


def _word_overlap(a: str, b: str) -> float:
    stop = {"the", "a", "an", "of", "and", "or", "inc", "llc", "ltd", "corp", "co"}
    wa = set(a.lower().split()) - stop
    wb = set(b.lower().split()) - stop
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def fetch_similar_clients(
    industry: str,
    msp_id: str | None,
    client_name: str,
    lat: float | None,
    lon: float | None,
) -> list[dict]:
    engine = _engine()
    with engine.connect() as conn:
        df = pd.read_sql(_QUERY, conn)
    engine.dispose()

    if df.empty:
        return []

    target_industry = normalize_industry(industry)

    rows = []
    for _, row in df.iterrows():
        raw_ind = row.get("industry") or row.get("industry_other") or ""
        client_industry = normalize_industry(str(raw_ind))
        score = 0.0

        # --- Industry (primary, up to 50 pts) ---
        if client_industry == target_industry:
            score += 50.0
        else:
            # Partial credit for related broad groups
            _related = {
                "Food & Beverage": {"Hospitality", "Events", "Corporate Dining / Workplace"},
                "Events": {"Hospitality", "Food & Beverage", "Entertainment & Media"},
                "Hospitality": {"Events", "Food & Beverage"},
                "Healthcare / Senior Living": {"Nonprofit / Social Services", "Residential / Facilities"},
                "Corporate Dining / Workplace": {"Food & Beverage", "Events"},
            }
            if target_industry in _related and client_industry in _related[target_industry]:
                score += 15.0

        # --- MSP match (up to 20 pts) ---
        if msp_id and str(row.get("msp_id") or "") == str(msp_id):
            score += 20.0

        # --- Name similarity (up to 10 pts) ---
        if client_name:
            score += _word_overlap(client_name, str(row.get("client_name") or "")) * 10.0

        # --- Activity (up to 15 pts) — prefers active clients ---
        shifts = int(row.get("shifts_last_year") or 0)
        if shifts > 0:
            score += min(15.0, math.log(shifts + 1) * 4)

        # --- Location proximity (up to 5 pts bonus) ---
        miles: float | None = None
        client_lat = row.get("latitude")
        client_lon = row.get("longitude")
        if lat is not None and lon is not None and client_lat and client_lon:
            try:
                miles = _haversine_miles(lat, lon, float(client_lat), float(client_lon))
                if miles < 5:
                    score += 5.0
                elif miles < 15:
                    score += 3.0
                elif miles < 30:
                    score += 1.0
            except (ValueError, TypeError):
                miles = None

        last_shift = row.get("last_shift_date")
        last_shift_str = str(last_shift) if last_shift and str(last_shift) not in ("NaT", "None", "nan") else None

        rows.append({
            "client_name": row.get("client_name"),
            "industry": client_industry,
            "msp": row.get("msp_name") or "No MSP",
            "shifts_last_year": shifts,
            "last_shift_date": last_shift_str,
            "miles": round(miles, 1) if miles is not None else None,
            "city": row.get("city") or "",
            "state": row.get("state") or "",
            "_score": score,
        })

    rows.sort(key=lambda x: x["_score"], reverse=True)
    for r in rows:
        del r["_score"]
    return rows[:10]


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

async def get_msps_async() -> list[dict]:
    return await run_in_threadpool(fetch_msps)


async def get_similar_clients_async(
    industry: str,
    msp_id: str | None,
    client_name: str,
    lat: float | None,
    lon: float | None,
) -> list[dict]:
    return await run_in_threadpool(fetch_similar_clients, industry, msp_id, client_name, lat, lon)

# Shift Risk Monitoring Dashboard — Development Specification

## 1. Overview & Purpose

This app reads from the **same production database** as the GoLive staffing platform (MySQL, `cstaffing_live`) via a read-only replica connection (using the `REPORTABLE_DB_*` env vars already established in the project). It is a **monitoring tool only** — it never writes to the database.

The dashboard answers one question: **"Which published shifts across all clients are at risk of not being fully staffed before they start?"**

It surfaces only at-risk events and shifts, grouped by event, sorted by urgency. Shifts that are filling at a healthy rate are intentionally hidden.

---

## 2. Hierarchy & Key Business Rules

```
Client → Venue → Event → Shift → ShiftPosition (spots)
                                       ↓
                               shift_employee (person filling a spot)
```

| Concept | Definition |
|---|---|
| **Client** | The organization that hired GoLive |
| **Venue** | A physical location belonging to a client |
| **Event** | A single workday/job grouping shifts together; has a `date` |
| **Shift** | A block of time within an event (e.g. 8am–5pm); one event can have multiple shifts |
| **ShiftPosition** | A role within a shift (e.g. "Cook × 3"). The `count` column is the number of spots needed |
| **Spot** | One seat within a ShiftPosition. Total spots = `SUM(shift_position.count)` |
| **Published** | A shift position where `shift_position.was_published != 0`. Only published positions are visible to employees and should be tracked |
| **Filled spot** | A `shift_employee` record where `cancel_reason = 0` (no cancellation) and `deleted_at IS NULL` |
| **Confirmed spot** | A filled spot where additionally `confirmed = 1` |
| **Open spot** | `shift_position.count - filled_spots_for_that_position` |

### `shift_position.was_published` Values
| Value | Constant | Meaning |
|---|---|---|
| `0` | `WAS_PUBLISHED_NO` | Never published — **exclude from dashboard** |
| `1` | `WAS_PUBLISHED_INIT` | Queued/initiated for publishing |
| `2` | `WAS_PUBLISHED_DONE` | Fully processed and sent to employees |

Only positions with `was_published IN (1, 2)` should appear on this dashboard.

### `shift_employee.cancel_reason` — Active vs Cancelled
A `shift_employee` record counts as **active** (filling a spot) only when `cancel_reason = 0`. Any non-zero value means the employee was removed from that spot. Key values:
- `0` = Active (not cancelled)
- `2` = Employee cancelled < 24 hours
- `3` = Employee cancelled > 24 hours
- `4` = Client cancelled shift
- `5` = Client decreased staff

### `shift_employee.confirmed`
- `0` = Requested but not yet confirmed
- `1` = Confirmed

Both confirmed and unconfirmed active records (`cancel_reason = 0`) count toward filling a spot.

---

## 3. Required Database Tables & Fields

### `client`
| Column | Type | Use |
|---|---|---|
| `client_id` | bigint PK | Join key |
| `name` | varchar(255) | Display: client name on dashboard |

### `venue`
| Column | Type | Use |
|---|---|---|
| `venue_id` | bigint PK | Join key |
| `client_id` | bigint FK | Links venue to client |
| `name` | varchar(255) | Display: venue name |
| `staffing_manager_id` | int FK → `user.id` | To identify the responsible manager |
| `deleted_at` | timestamp | Filter: exclude soft-deleted venues |

### `event`
| Column | Type | Use |
|---|---|---|
| `event_id` | bigint PK | Join key; grouping unit for dashboard |
| `client_id` | bigint FK | Links to client |
| `venue_id` | bigint FK | Links to venue |
| `title` | varchar(100) | Display: event name |
| `date` | date | **Critical**: used for future filtering and risk deadline calculation |
| `deleted_at` | timestamp | Filter: exclude soft-deleted events |

### `shift`
| Column | Type | Use |
|---|---|---|
| `shift_id` | bigint PK | Join key |
| `event_id` | bigint FK | Groups shifts under an event |
| `start` | datetime | Shift start time (used for display and time-to-start calc) |
| `end` | datetime | Shift end time (display) |
| `deleted_at` | timestamp | Filter: exclude soft-deleted shifts |

### `shift_position`
| Column | Type | Use |
|---|---|---|
| `shift_position_id` | bigint PK | Join key |
| `shift_id` | bigint FK | Links position to shift |
| `position_id` | bigint FK | Links to position role |
| `rate` | decimal | Display: employee pay rate |
| `bill_rate` | decimal | Display: client bill rate |
| `count` | int | **Critical**: total spots needed for this position |
| `was_published` | tinyint(1) | Filter: must be `!= 0` to appear on dashboard |
| `filled` | tinyint | Current filled flag (0 or 1); managed by app |
| `created_at` | timestamp | **Critical**: when the position was created — used as the start of the fill-rate window |
| `deleted_at` | timestamp | Filter: exclude soft-deleted positions |

### `position`
| Column | Type | Use |
|---|---|---|
| `position_id` | bigint PK | Join key |
| `description` | varchar(100) | Display: position name (e.g. "Cook", "Server") |

### `shift_employee`
| Column | Type | Use |
|---|---|---|
| `shift_employee_id` | bigint PK | Join key |
| `shift_position_id` | bigint FK | Which position slot this fills |
| `event_id` | bigint FK | Denormalized event ref |
| `employee_id` | bigint FK | The employee assigned |
| `cancel_reason` | int | **Critical**: `0` = active; any other value = cancelled/removed |
| `confirmed` | tinyint | `1` = confirmed; `0` = pending |
| `created_at` | timestamp | When the employee first signed up for this spot |
| `confirmed_at` | timestamp | When the spot was confirmed |
| `cancelled_at` | timestamp | When the spot was cancelled |
| `deleted_at` | timestamp | Soft-delete flag |

### `publishing`
| Column | Type | Use |
|---|---|---|
| `id` | bigint PK | Join key |
| `client_id` | bigint | Which client published |
| `event_id` | bigint | Which event was published |
| `created_at` | timestamp | **Critical**: when admin clicked "Publish" — earliest possible fill timestamp |
| `processed` | timestamp | When the publish job actually ran |
| `deleted_at` | timestamp | Filter: exclude soft-deleted publishings |

> **Note on T0**: Use the **earliest `publishing.created_at`** for the event as `T0` (the moment the shift became visible to employees). If no `publishing` record exists, fall back to `shift_position.created_at`.

### `publish_employee`
| Column | Type | Use |
|---|---|---|
| `id` | int PK | — |
| `employee_id` | int | Which employee was published to |
| `shift_position_id` | int | Which position |
| `event_id` | int | Which event |
| `publishing_id` | bigint | Which publish action |
| `created_on` | timestamp | When published to this employee |

> Used for the **timeline chart**: tracking when each employee received the publish notification.

---

## 4. Core Risk Algorithm

### 4.1 Variables Per ShiftPosition

For each published shift position in a future event, calculate:

| Variable | Formula |
|---|---|
| `total_spots` | `shift_position.count` |
| `filled_spots` | `COUNT(shift_employee) WHERE cancel_reason = 0 AND deleted_at IS NULL` |
| `open_spots` | `total_spots - filled_spots` |
| `T0` | Earliest `publishing.created_at` for the event (or `shift_position.created_at`) |
| `T_now` | Current timestamp |
| `T_deadline` | `shift.start - 24 hours` |
| `total_window_hours` | `(T_deadline - T0)` in hours |
| `elapsed_hours` | `(T_now - T0)` in hours |
| `remaining_hours` | `(T_deadline - T_now)` in hours |
| `fill_rate` | `filled_spots / elapsed_hours` (spots per hour since publish) |
| `projected_fill` | `fill_rate * total_window_hours` |
| `fill_pct` | `filled_spots / total_spots * 100` |

### 4.2 Risk Determination

A shift position is **at risk** when **all** of the following are true:

1. `event.date >= TODAY`
2. `shift_position.was_published != 0`
3. `event.deleted_at IS NULL AND shift.deleted_at IS NULL AND shift_position.deleted_at IS NULL`
4. `open_spots > 0`
5. **At least one risk condition is met:**

**Risk Condition A — Insufficient fill rate:**
```
projected_fill < total_spots  AND  remaining_hours > 0  AND  elapsed_hours >= 1
```

**Risk Condition B — Deadline passed with open spots:**
```
remaining_hours <= 0  AND  open_spots > 0
```

**Risk Condition C — Emergency (shift within 48 hours with any open spots):**
```
hours_until_shift <= 48  AND  open_spots > 0
```

### 4.3 Risk Score (for Sorting)

```python
def calculate_risk_score(open_spots, total_spots, remaining_hours, projected_fill):
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
```

---

## 5. Dashboard Display Logic

### 5.1 What to Show / Hide

- **Show**: Events with >= 1 at-risk shift position
- **Hide**: Events where all positions are on track
- **Hide**: Past events (`event.date < TODAY`)
- **Hide**: Cancelled/deleted events
- **Hide**: Unpublished positions (`was_published = 0`)

### 5.2 Sort Order

1. **Events**: By `event.date ASC`, then by max risk score DESC
2. **Shifts within event**: By `shift.start ASC`
3. **Positions within shift**: By risk score DESC

### 5.3 Event Card Header

- Client name, venue name, event title, event date
- Days until event
- Total open spots / total needed across all at-risk positions in this event

### 5.4 Shift Position Row

| Field | Source |
|---|---|
| Position name | `position.description` |
| Shift window | `shift.start -> shift.end` |
| Spots needed | `shift_position.count` |
| Spots filled | `filled_spots` |
| Spots open | `open_spots` |
| Pay rate | `$shift_position.rate/hr` |
| Fill % | Progress bar |
| Hours to deadline | `remaining_hours` |
| Risk badge | CRITICAL / WARNING / AT RISK |
| Published date | Earliest `publishing.created_at` |

### 5.5 Fill-Rate Timeline Chart (Expandable per Position)

- **X-axis**: Time from `T0` to `shift.start`
- **Y-axis**: Cumulative employees signed up (0 to `total_spots`)
- **Actual line**: Step function built from `shift_employee.created_at` timestamps (active records only)
- **Target line**: Straight line from `(T0, 0)` to `(T_deadline, total_spots)`
- **Annotations**: Vertical "NOW" line and vertical "DEADLINE" line

### 5.6 Risk Badge Colors

| Level | Condition | Color |
|---|---|---|
| CRITICAL | `remaining_hours <= 24` or past deadline | `#EF4444` |
| WARNING | `remaining_hours <= 72` | `#F59E0B` |
| AT RISK | `remaining_hours > 72`, fill rate insufficient | `#3B82F6` |

---

## 6. API Endpoints

### `GET /shift-risk-dashboard/`
Returns the HTML page.

### `GET /shift-risk-dashboard/data`
Full JSON payload — events, shifts, positions, risk metrics, timeline data.

**Response shape:**
```json
{
  "generated_at": "2026-04-28T09:00:00",
  "total_at_risk_events": 12,
  "total_open_spots": 47,
  "events": [
    {
      "event_id": 12345,
      "event_date": "2026-05-01",
      "event_title": "Spring Gala",
      "client_name": "UCLA",
      "venue_name": "Royce Hall",
      "days_until_event": 3,
      "total_open_spots": 5,
      "total_spots": 8,
      "max_risk_score": 87.4,
      "shifts": [
        {
          "shift_id": 999,
          "shift_start": "2026-05-01T08:00:00",
          "shift_end": "2026-05-01T17:00:00",
          "positions": [
            {
              "shift_position_id": 777,
              "position_name": "Cook",
              "total_spots": 3,
              "filled_spots": 1,
              "open_spots": 2,
              "pay_rate": 22.50,
              "bill_rate": 32.00,
              "fill_pct": 33.3,
              "fill_rate_per_hour": 0.04,
              "projected_fill": 1.8,
              "hours_until_deadline": 46.5,
              "hours_until_shift": 70.5,
              "published_at": "2026-04-20T10:00:00",
              "risk_score": 87.4,
              "risk_level": "WARNING",
              "is_past_deadline": false,
              "timeline": [
                {"timestamp": "2026-04-20T10:00:00", "cumulative_filled": 0},
                {"timestamp": "2026-04-22T14:30:00", "cumulative_filled": 1}
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

### `GET /shift-risk-dashboard/summary`
Lightweight counts for a status badge:
```json
{
  "total_at_risk_events": 12,
  "critical_events": 3,
  "warning_events": 6,
  "at_risk_events": 3,
  "total_open_spots": 47
}
```

---

## 7. Core SQL Queries

### 7.1 Main At-Risk Positions Query

```sql
SELECT
    c.client_id,
    c.name                                          AS client_name,
    v.venue_id,
    v.name                                          AS venue_name,
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
GROUP BY
    c.client_id, v.venue_id, e.event_id, s.shift_id,
    sp.shift_position_id, p.position_id
ORDER BY e.date ASC, s.start ASC
```

> Apply the risk algorithm **in Python** after fetching rows. Do not compute risk score in SQL.

### 7.2 Timeline Data Query (Per Position, On Demand)

```sql
SELECT
    se.created_at       AS signup_timestamp,
    se.confirmed_at,
    se.shift_employee_id
FROM shift_employee se
WHERE se.shift_position_id = :shift_position_id
  AND se.cancel_reason = 0
  AND se.deleted_at IS NULL
ORDER BY se.created_at ASC
```

Build the cumulative fill array in Python by iterating rows and incrementing a counter at each `signup_timestamp`.

---

## 8. Polling & Refresh Architecture

- On page load, fetch `/shift-risk-dashboard/data` and render
- `setInterval` re-fetch every **5 minutes** (300,000 ms)
- Show "Last updated X minutes ago" — update display every 60 seconds
- On refresh: re-render in place; animate removal of positions that are now safe
- **Backend cache**: Cache main query result for 2 minutes using a simple dict with a TTL timestamp to avoid overloading the replica DB

---

## 9. App Integration Pattern

Follows the established `GoLive_Staffing` project conventions:

### Files to Create
```
apps/
└── shift_risk_dashboard/
    ├── __init__.py          # from .views import router
    └── views.py             # FastAPI router, SQL queries, risk algorithm

templates/
└── apps/
    └── shift_risk_dashboard.html   # Full UI (HTML + CSS + JS inline)
```

### Registration in `app.py`
```python
from apps.shift_risk_dashboard import router as shift_risk_dashboard_router
app.include_router(shift_risk_dashboard_router, prefix="/shift-risk-dashboard")
```

### DB Connection
Use the identical `_db_url_from_env()` and `_engine()` pattern from `staffing_coverage_monitor/views.py`. Prefers `REPORTABLE_DB_HOST` (replica) over `DB_HOST` (primary) automatically.

No new Python packages are required — `pandas`, `sqlalchemy`, `fastapi`, and `pymysql` are already in `requirements.txt`.

---

## 10. Edge Cases & Business Logic Notes

| Scenario | Handling |
|---|---|
| Event is today | Set `T_deadline = shift.start`; escalate all open spots to CRITICAL |
| `elapsed_hours < 1` (just published) | Skip fill-rate projection; still show if within 48h emergency window |
| Unconfirmed but active spots (`confirmed=0, cancel_reason=0`) | Count as filled — mirrors GoLive's `countValidShiftEmployees()` logic |
| `shift_position.count = 0` | Skip entirely |
| Some shifts in event are safe, some at risk | Show event card; only display the at-risk shift positions within it |
| `publishing.deleted_at IS NOT NULL` | Ignore deleted publish records; use `shift_position.was_published` as the truth source |
| No `publishing` record for event | Fall back to `shift_position.created_at` as `T0` |
| `projected_fill >= total_spots` | On track — exclude from dashboard |
| Position becomes fully filled mid-session | Remove from dashboard on next refresh cycle |

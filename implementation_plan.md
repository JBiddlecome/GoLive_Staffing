# Sales Rate Intelligence Dashboard

This document outlines the approach for building the Sales Rate Intelligence Dashboard, designed to give Sales both a quick quoting answer and enough supporting context to trust the rate data.

## Goal Description
Build a new standalone app in the Sales Tools section (or a new section as appropriate) that provides a Sales Rate Intelligence Dashboard. This dashboard will allow users to analyze historical and current pay/bill rates for given positions, counties, and industries over a specified time window. The dashboard will show averages, percentile distributions, and provide a list of comparable accounts to aid in sales quoting.

## User Review Required
> [!IMPORTANT]
> **Data Grain and Aggregation:** Because a client can have multiple venues in the same (or different) counties, joining rates to venues will duplicate the rate rows. To accurately calculate averages and percentiles, the system will filter to the selected county (and other dimensions), and then **deduplicate on the rate record ID** (`client_position_amount.id`). This ensures a client's rate is only weighted once in the averages, even if they have 10 venues in that county. **Please confirm if this is the desired behavior for statistical weighting.**

> [!IMPORTANT]
> **Performance:** We plan to use a SQL query to fetch the raw expanded data into Pandas for on-the-fly deduplication and percentile calculation (since calculating proper percentiles in older MySQL versions is difficult, and Pandas excels at this). The queried view will be heavily cached or materialized in code to maintain sub-second UI response times.

## Proposed Changes

### Database Query Layer
We will define a core SQL extraction that constructs `fact_sales_rate_benchmark` virtually.

#### [NEW] `apps/sales_rate_intelligence/database.py`
- Establish the query that joins `client_position_amount` -> `client_position` -> `client` -> `position` -> `venue` -> `county`.
- Alias all needed dimensions (`bill_rate`, `pay_rate`, `surcharge`, `sales_executive_id`, `bundle`, `won_date`, etc.).
- Convert overlapping date range logic to properly filter active rate periods (`start_date` and `end_date`).
- Normalization mapping for `industry` and `industry_other` (e.g. standardizing "Hospitals" vs "Hospital").

### Backend and APIs
We will create a FastAPI router to serve the dashboard and API endpoints.

#### [MODIFY] `app.py`
- Mount the new `sales_rate_intelligence_router` under `/sales-rate-intelligence`.

#### [NEW] `apps/sales_rate_intelligence/views.py`
- Fast API router definitions.
- `GET /`: Renders the Jinja template.
- `GET /api/benchmark`: Accepts query parameters (`position_id`, `county_id`, `industry`, `date_start`, `date_end`, etc.).
- Pandas-based aggregation logic: deduplicates the raw query by rate ID, calculates mean/median/min/max/25th/75th percentiles.
- Calculate spread (`bill_rate - pay_rate`) and markup `%`.
- Generates "low sample size" warnings if matching clients < 5 or records < 10.
- Returns JSON for the KPI cards, Benchmark percentiles, and rows for the Comparable Accounts table.

### Frontend Dashboard
We will build a high-aesthetics, modern UI using Vanilla HTML/JS, taking inspiration from glassmorphism and modern dark mode designs.

#### [NEW] `templates/apps/sales_rate_intelligence.html`
- **Filter Panel**: Dropdowns for Position, County, Industry, Date Range (default last 12 mos).
- **KPI Cards**: Dynamic cards showing Avg Bill, Avg Pay, Avg Spread, Avg Markup %, # of Clients, # of Rate Records. Include a warning banner if data is thin.
- **Benchmark Panel**: Visual distribution showing Min, 25th (Conservative), Median (Market), 75th (Premium), Max.
- **Comparables Table**: Drill-down table listing Client, Industry, County, Position, Bill/Pay rates, Surcharge, Dates, Rep, Bundle, Won date.
- **Visual Design**: Vibrant gradient accents, deep modern colors, micro-interactions on hover, rounded cards, and clean typography.

#### [NEW] `static/css/sales_rate_intelligence.css`
- Custom, premium design tokens (colors, typography).
- Responsive grid and flexbox layouts.

#### [NEW] `static/js/sales_rate_intelligence.js`
- Alpine.js or vanilla JS for state management.
- Fetches data from `/api/benchmark` dynamically when filters change.
- Handles CSV exports.

## Open Questions
- Is there an existing materialized cache implementation in this repository that we should use, or should we just use a Python-level `functools.lru_cache` on the raw SQL extraction with a short TTL (e.g. 5 minutes) since this is a read-only dashboard?
- I will start with Phase 1 (Configured rates) and Phase 3 (MVP Dashboard features). Features like Monthly Trend Charts and Rate history audits (Phase 4) are pushed to the next iteration unless you need them now. Is this scope acceptable?

## Verification Plan
### Automated Tests
- Validate that the endpoint correctly parses date ranges and filters overlapping periods.
- Validate that statistical calculations match a manual pandas calculation.
- Run `python -m py_compile app.py` to ensure no syntax errors on startup.

### Manual Verification
1. Access the dashboard UI at `/sales-rate-intelligence`.
2. Select a common position (e.g. "Dishwasher") and County (e.g. "Orange County").
3. Verify that KPI calculations match what is roughly expected.
4. Verify that deduplication correctly avoids skewing averages when a client has many venues.
5. Emulate low volume and verify warning trigger.

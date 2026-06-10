# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoLive Staffing Tools is an internal web platform for GoLive Staffing (culinary/hospitality temp staffing agency). It provides 50+ specialized tools across recruiting, HR, payroll, sales reporting, and operations. Access is restricted to `@culinarystaffing.com` and `@golivestaffing.com` email domains.

## Running the App

```bash
# Install dependencies
pip install -r requirements.txt
python -m playwright install chromium  # required for Profile Creator automation

# Run locally
uvicorn app:app --reload
# App available at http://localhost:8000
```

Production runs on Render via Gunicorn + Uvicorn workers, with an SSH tunnel from Render → EC2 bastion → private RDS (MariaDB on port 3307 locally → 3306 on RDS).

## Tech Stack

- **Backend**: FastAPI + Uvicorn/Gunicorn, Python 3.12.6
- **Frontend**: Jinja2 templates, HTMX, minimal vanilla JS
- **Database**: MariaDB (`cstaffing_live`) via SQLAlchemy + PyMySQL
- **Data processing**: Pandas, OpenPyXL, xlsxwriter, DuckDB, PyMuPDF
- **External**: OpenAI API, Playwright (browser automation)
- **Deployment**: Render (`render.yaml`, `Procfile`, `scripts/render_start.sh`)

## Architecture

### App Structure

Each tool lives in `apps/<tool_name>/` and follows this pattern:
- `views.py` — FastAPI route handlers (the router)
- `__init__.py`
- Optional: `models.py`, `database.py`, `scheduler.py`, `cli.py`

All routers are registered in `app.py` (500+ lines, 60+ routers). New apps must be imported and mounted there.

### Background Schedulers

13+ asyncio background tasks launch in the FastAPI lifespan context manager in `app.py`. These handle: MSP monitoring, credit card client tracking, position requests, AR contacts, email forwarding, certificate approvals, orders inbox processing, etc. Each scheduler lives in its app's `scheduler.py`.

### Data Persistence

Mixed persistence strategy:
- **MariaDB** (`cstaffing_live`) for core staffing domain data — accessed via SQLAlchemy ORM and raw SQL
- **SQLite** `.db` files in `data/` for some app-specific storage
- **JSON documents** in `data/` for derived data (version-controlled): `position_requests.json`, `orders_inbox.json`, `new_employee_position_approver.json`

### Database Domain Model

`client → venue → event → shift → shift_position → shift_employee → employee/timesheet`

Key tables: `client`, `venue`, `event`, `shift`, `shift_position`, `shift_employee`, `employee`, `timesheet`, `client_position_amount` (pay rates).

**Soft deletes**: Active records always filtered by `WHERE deleted_at IS NULL` on client, venue, event, shift, shift_position, shift_employee, employee.

Full schema and join paths are documented in `golive_database_reference.md`.

### Authentication

Session-based auth middleware. Unauthenticated requests redirect to `/auth/login`. Auth logic lives in `apps/auth/`.

## Key Files

- `app.py` — main entry point, all router mounts, lifespan scheduler startup
- `golive_database_reference.md` — complete DB schema reference (read this before writing queries)
- `apps/context.py` — shared app context/utilities
- `data/sales_staffing_metrics.csv` — committed derived metrics (regenerate + commit together with the workbook)

## Sales Metrics CLI

The sales/staffing Excel workbook is regenerated from the CSV via:

```bash
python -m apps.sales_staffing_metrics.cli --metrics data/sales_staffing_metrics.csv --workbook "data/Sales and Staffing Charts.xlsx"
```

Both `sales_staffing_metrics.csv` and `Sales and Staffing Charts.xlsx` are committed together.

## Environment Variables

Required: DB connection (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`), SSH bastion tunnel config, `OPENAI_API_KEY`, and Render deployment settings.

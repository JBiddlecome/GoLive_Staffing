import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import json

# Load env
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

def _db_url_from_env() -> URL:
    host = os.getenv("REPORTABLE_DB_HOST") or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306"))
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

# Blacklisted columns per table — never exposed to the AI
BLACKLIST = {
    "employee": ["sex", "dob", "ssn"]
}

# Tables excluded entirely — AI cannot query or know about these
EXCLUDED_TABLES = {
    "oauth_access_tokens",
    "oauth_authorization_codes",
    "oauth_clients",
    "oauth_jwt",
    "oauth_public_keys",
    "oauth_refresh_tokens",
    "oauth_scopes",
    "oauth_users",
    "session",
    "migration",
    "golive_app_state",
}

# Primary tables (loaded into DuckDB on startup) vs secondary (schema-only context)
PRIMARY_TABLES = [
    "client", "venue", "event", "shift", "employee",
    "shift_position", "shift_employee", "certification",
    "venue_certification", "timesheet", "user"
]

def get_schema():
    engine = create_engine(_db_url_from_env())
    schema = {}
    all_tables = []

    try:
        with engine.connect() as conn:
            # Get the database name
            db_name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")

            # Fetch all table names
            result = conn.execute(text(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = :db AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME"
            ), {"db": db_name})
            all_tables = [row[0] for row in result]
            print(f"Found {len(all_tables)} tables total.")

            # Fetch columns for every table
            for table in all_tables:
                # Skip entirely excluded tables
                if table in EXCLUDED_TABLES:
                    continue
                try:
                    res = conn.execute(text(f"DESCRIBE `{table}`")).fetchall()
                    columns = []
                    for row in res:
                        col_name = row[0]
                        col_type = str(row[1])
                        if table in BLACKLIST and col_name in BLACKLIST[table]:
                            continue
                        columns.append({
                            "name": col_name,
                            "type": col_type,
                            "primary": table in PRIMARY_TABLES
                        })
                    schema[table] = columns
                except Exception as e:
                    print(f"  WARN: Could not describe table '{table}': {e}")
    finally:
        engine.dispose()

    return schema, all_tables

if __name__ == "__main__":
    schema, all_tables = get_schema()
    included_tables = [t for t in all_tables if t not in EXCLUDED_TABLES]

    output_path = "apps/reports/dynamic_schema.json"
    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"\nSchema saved to {output_path}")
    print(f"  Total tables in DB: {len(all_tables)}")
    print(f"  Excluded (security): {len(EXCLUDED_TABLES)}")
    print(f"  Tables in schema: {len(included_tables)}")
    print(f"  Primary tables: {len([t for t in included_tables if t in PRIMARY_TABLES])}")
    print(f"  Secondary tables: {len([t for t in included_tables if t not in PRIMARY_TABLES])}")
    print(f"  Blacklisted columns filtered: employee.{{sex, dob, ssn}}")
    print("\nAll included tables:")
    for t in included_tables:
        marker = " [PRIMARY]" if t in PRIMARY_TABLES else ""
        col_count = len(schema.get(t, []))
        print(f"  - {t} ({col_count} cols){marker}")
    print(f"\nExcluded tables ({len(EXCLUDED_TABLES)}):")
    for t in sorted(EXCLUDED_TABLES):
        print(f"  - {t}")

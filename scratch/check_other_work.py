import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(url)

START_DATE = "2026-04-20"
END_DATE   = "2026-04-26"

with engine.begin() as conn:

    # 1. Show all columns in employee_other_work
    cols = conn.execute(text("SHOW COLUMNS FROM employee_other_work")).fetchall()
    print("=== employee_other_work columns ===")
    for c in cols:
        print(f"  {c[0]:<30} {c[1]}")

    print()

    # 2. All rows in the date range (no filters)
    df_all = pd.read_sql(
        text("SELECT * FROM employee_other_work WHERE date >= :s AND date <= :e ORDER BY date"),
        conn, params={"s": START_DATE, "e": END_DATE}
    )
    print(f"=== All rows in {START_DATE} → {END_DATE} (no filters): {len(df_all)} rows ===")
    print(df_all.to_string())
    print()

    # 3. Current query total (SUM cost, no other filters)
    total_raw = conn.execute(
        text("SELECT SUM(cost) FROM employee_other_work WHERE date >= :s AND date <= :e"),
        {"s": START_DATE, "e": END_DATE}
    ).scalar()
    print(f"SUM(cost) — current app query result : {total_raw}")

    # 4. Check if there's a deleted_at / active / status column that might need filtering
    col_names = [c[0] for c in cols]
    soft_delete_cols = [c for c in col_names if any(k in c.lower() for k in ['delet', 'active', 'status', 'void', 'cancel'])]
    if soft_delete_cols:
        print(f"\nPossible soft-delete/status columns found: {soft_delete_cols}")
        for sc in soft_delete_cols:
            dist = conn.execute(
                text(f"SELECT {sc}, COUNT(*) as cnt FROM employee_other_work "
                     f"WHERE date >= :s AND date <= :e GROUP BY {sc}"),
                {"s": START_DATE, "e": END_DATE}
            ).fetchall()
            print(f"  {sc} value distribution: {dist}")
    else:
        print("\nNo obvious soft-delete/status columns found.")

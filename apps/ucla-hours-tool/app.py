import io
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------
# Helper functions
# -----------------------------

def read_any_table(file):
    """Read .xlsx or .csv into a pandas DataFrame. Trim header whitespace."""
    name = file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(file)
    else:
        # Try pandas/openpyxl first; fall back to pyxlsb/calamine if available
        try:
            df = pd.read_excel(file, engine="openpyxl")
        except Exception:
            # Try no-engine (lets pandas pick one)
            file.seek(0)
            df = pd.read_excel(file)
    # normalize headers
    df.columns = [str(c).strip() for c in df.columns]
    return df


def infer_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        # exact lower-case match
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
        # fallback: contains search
        for k, v in cols_lower.items():
            if cand.lower() in k:
                return v
    return None


def most_recent_sunday(anchor: date | None = None) -> date:
    if anchor is None:
        anchor = date.today()
    # Monday=0 ... Sunday=6
    dow = anchor.weekday()
    # distance to previous Sunday
    days_back = (dow + 1) % 7 + 0  # if already Sunday -> go back 0 days
    # Simpler: step back until Sunday
    d = anchor
    while d.weekday() != 6:
        d -= timedelta(days=1)
    return d


def normalize_name_parts(df: pd.DataFrame, first_col: str | None, last_col: str | None, full_col: str | None) -> pd.Series:
    if first_col and last_col and first_col in df.columns and last_col in df.columns:
        first = df[first_col].fillna("").astype(str).str.strip()
        last = df[last_col].fillna("").astype(str).str.strip()
        names = (first + " " + last).str.strip()
        if full_col and full_col in df.columns:
            fallback = df[full_col].fillna("").astype(str).str.strip()
            names = names.mask(names.eq(""), fallback)
    elif full_col and full_col in df.columns:
        names = df[full_col].astype(str)
    else:
        # last resort: try to find something that looks like a name
        guessed = infer_column(df, ["employee name", "name"])
        if guessed:
            names = df[guessed].astype(str)
        else:
            raise ValueError("Could not determine employee name columns. Please map them in the sidebar.")
    # canonicalize spacing/case for matching
    return names.str.replace(r"\s+", " ", regex=True).str.strip()


# -----------------------------
# UI
# -----------------------------

st.set_page_config(page_title="UCLA Assignment Exporter", layout="wide")
st.title("UCLA Assignment Exporter")

st.markdown(
    """
    Upload a **Payroll** spreadsheet and the **Sample Assignment List UCLA** spreadsheet.
    The app will:
    1. Filter payroll rows to only clients containing **"UCLA"** in the client name.
    2. Sum hours = **Reg H (e) + OT H (e) + DT H (e)** per **Employee** and **Pay Rate**.
    3. Look up the matching **Assign No** from the assignment list by **Employee Name + Pay Rate**.
    4. Output an Excel with columns: **Assignment #, Employee Name, Pay Rate, Work Date, Weekending Date, Hours, Unique Line ID**.
    """
)

col1, col2 = st.columns(2)
with col1:
    payroll_file = st.file_uploader("Upload Payroll (.xlsx or .csv)", type=["xlsx", "csv"], key="payroll")
with col2:
    assign_file = st.file_uploader("Upload Sample Assignment List UCLA (.xlsx or .csv)", type=["xlsx", "csv"], key="assign")

if not payroll_file or not assign_file:
    st.info("Upload both files to continue.")
    st.stop()

# Read files
try:
    payroll_df = read_any_table(payroll_file)
    assign_df = read_any_table(assign_file)
except Exception as e:
    st.error(f"Failed to read one of the files: {e}")
    st.stop()

with st.expander("Preview – Payroll (first 10 rows)"):
    st.dataframe(payroll_df.head(10))
with st.expander("Preview – Assignment List (first 10 rows)"):
    st.dataframe(assign_df.head(10))

# Sidebar mapping
st.sidebar.header("Column Mapping (if auto-detect fails)")

client_col = infer_column(payroll_df, ["client", "client name", "venue", "customer"])
reg_col = infer_column(payroll_df, ["Reg H (e)", "Reg H", "reg hours", "regular hours"])
ot_col = infer_column(payroll_df, ["OT H (e)", "OT H", "ot hours", "overtime hours"])
dt_col = infer_column(payroll_df, ["DT H (e)", "DT H", "doubletime hours", "dt hours"])
payrate_col = infer_column(payroll_df, ["pay rate", "rate", "payrate", "pay_rate"]) 
first_col = infer_column(payroll_df, ["first name", "firstname", "emp first name"])
last_col  = infer_column(payroll_df, ["last name", "lastname", "emp last name"])
full_name_col = infer_column(payroll_df, ["employee name", "name", "emp name"]) 

assign_name_col = infer_column(assign_df, ["employee name", "name", "firstname lastname", "employee"])
assign_rate_col = infer_column(assign_df, ["pay rate", "rate", "payrate"]) 
assign_no_col   = infer_column(assign_df, ["assign no", "assignment #", "assignment", "assign #", "assignment number"]) 

client_col = st.sidebar.selectbox("Payroll: client column", payroll_df.columns, index=(list(payroll_df.columns).index(client_col) if client_col in payroll_df.columns else 0))
reg_col = st.sidebar.selectbox("Payroll: Reg hours column", payroll_df.columns, index=(list(payroll_df.columns).index(reg_col) if reg_col in payroll_df.columns else 0))
ot_col = st.sidebar.selectbox("Payroll: OT hours column", payroll_df.columns, index=(list(payroll_df.columns).index(ot_col) if ot_col in payroll_df.columns else 0))
dt_col = st.sidebar.selectbox("Payroll: DT hours column", payroll_df.columns, index=(list(payroll_df.columns).index(dt_col) if dt_col in payroll_df.columns else 0))
payrate_col = st.sidebar.selectbox("Payroll: Pay Rate column", payroll_df.columns, index=(list(payroll_df.columns).index(payrate_col) if payrate_col in payroll_df.columns else 0))

if first_col and last_col and first_col in payroll_df.columns and last_col in payroll_df.columns:
    default_name_index = 1
elif full_name_col and full_name_col in payroll_df.columns:
    default_name_index = 0
else:
    default_name_index = 0

name_mode = st.sidebar.radio("Payroll: name source", ["Full name column", "First + Last"], index=default_name_index)
if name_mode == "Full name column":
    full_name_col = st.sidebar.selectbox("Payroll: full name column", payroll_df.columns, index=(list(payroll_df.columns).index(full_name_col) if full_name_col in payroll_df.columns else 0))
    first_col = last_col = None
else:
    full_name_col = None
    first_col = st.sidebar.selectbox("Payroll: first name column", payroll_df.columns, index=(list(payroll_df.columns).index(first_col) if first_col in payroll_df.columns else 0))
    last_col  = st.sidebar.selectbox("Payroll: last name column", payroll_df.columns, index=(list(payroll_df.columns).index(last_col) if last_col in payroll_df.columns else 0))

assign_name_col = st.sidebar.selectbox("Assignment: employee name column", assign_df.columns, index=(list(assign_df.columns).index(assign_name_col) if assign_name_col in assign_df.columns else 0))
assign_rate_col = st.sidebar.selectbox("Assignment: pay rate column", assign_df.columns, index=(list(assign_df.columns).index(assign_rate_col) if assign_rate_col in assign_df.columns else 0))
assign_no_col   = st.sidebar.selectbox("Assignment: Assign No column", assign_df.columns, index=(list(assign_df.columns).index(assign_no_col) if assign_no_col in assign_df.columns else 0))

# Optional: override weekending date
default_we = most_recent_sunday()
custom_we = st.sidebar.date_input("Weekending date (most recent Sunday by default)", value=default_we)
we_str = custom_we.strftime("%m/%d/%Y")
we_id_prefix = custom_we.strftime("%Y%m%d")

# -----------------------------
# Transformations
# -----------------------------

# 1) Filter to UCLA clients (case-insensitive substring)
payroll_df[client_col] = payroll_df[client_col].astype(str)
mask_ucla = payroll_df[client_col].str.contains("UCLA", case=False, na=False)
payroll_ucla = payroll_df.loc[mask_ucla].copy()

# 2) Build employee full name
payroll_ucla["Employee Name"] = normalize_name_parts(payroll_ucla, first_col, last_col, full_name_col)

# 3) Hours sum per employee + pay rate
for c in (reg_col, ot_col, dt_col):
    if c not in payroll_ucla.columns:
        raise ValueError(f"Missing hours column: {c}")

def to_num(x):
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return np.nan

payroll_ucla[reg_col] = payroll_ucla[reg_col].apply(to_num).fillna(0.0)
payroll_ucla[ot_col]  = payroll_ucla[ot_col].apply(to_num).fillna(0.0)
payroll_ucla[dt_col]  = payroll_ucla[dt_col].apply(to_num).fillna(0.0)

if payrate_col not in payroll_ucla.columns:
    raise ValueError("Could not find Pay Rate column in Payroll. Map it in the sidebar.")

payroll_ucla["Pay Rate"] = payroll_ucla[payrate_col].apply(to_num)
payroll_ucla["Hours"] = payroll_ucla[reg_col] + payroll_ucla[ot_col] + payroll_ucla[dt_col]

summary = (
    payroll_ucla
    .groupby(["Employee Name", "Pay Rate"], dropna=False, as_index=False)["Hours"].sum()
)

# 4) Prepare Assignment list for matching (normalize name + numeric rate)
assign_df = assign_df.copy()
assign_df["_name"] = (
    assign_df[assign_name_col]
    .astype(str)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.lower()
)
assign_df["_rate"] = assign_df[assign_rate_col].apply(to_num)
assign_df_slim = assign_df[["_name", "_rate", assign_no_col]].drop_duplicates()

# 5) Join to fetch Assignment #
summary["_name"] = (
    summary["Employee Name"]
    .astype(str)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.lower()
)
summary["_rate"] = summary["Pay Rate"].apply(to_num)
joined = summary.merge(
    assign_df_slim,
    how="left",
    left_on=["_name", "_rate"],
    right_on=["_name", "_rate"],
)

# 6) Build final output columns
joined.rename(columns={assign_no_col: "Assignment #"}, inplace=True)
joined["Work Date"] = we_str
joined["Weekending Date"] = we_str

# 7) Unique Line ID generation (YYYYMMDD0001, ...)
joined = joined.sort_values(["Employee Name", "Pay Rate"]).reset_index(drop=True)
joined["Unique Line ID"] = [f"{we_id_prefix}{i:04d}" for i in range(1, len(joined) + 1)]

final_cols = [
    "Assignment #", "Employee Name", "Pay Rate", "Work Date",
    "Weekending Date", "Hours", "Unique Line ID"
]
final = joined[final_cols]

st.subheader("Output preview")
st.dataframe(final)

# 8) Unmatched rows helper
unmatched = joined[joined["Assignment #"].isna()][["Employee Name", "Pay Rate", "Hours"]]
if not unmatched.empty:
    st.warning("Some rows could not be matched to an Assignment # (check name or pay rate differences). See below.")
    st.dataframe(unmatched)

# 9) Download as Excel
out_buf = io.BytesIO()
with pd.ExcelWriter(out_buf, engine="xlsxwriter") as xw:
    final.to_excel(xw, index=False, sheet_name="Export")
    if not unmatched.empty:
        unmatched.to_excel(xw, index=False, sheet_name="Unmatched")

st.download_button(
    label="Download Export (Excel)",
    data=out_buf.getvalue(),
    file_name=f"ucla_assignment_export_{we_id_prefix}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

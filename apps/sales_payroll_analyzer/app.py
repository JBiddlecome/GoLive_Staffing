from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_PATH = BASE_DIR / "payroll.xlsx"

REQUIRED_COLS = [
    "Date",
    "Position",
    "Pay Rate",
    "Bill Rate",
    "County of Venue",
    "Industry",
]


@st.cache_data(show_spinner=False)
def load_payroll_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Payroll workbook not found at {path}. Please commit payroll.xlsx to the repo root."
        )

    df = pd.read_excel(path, engine="openpyxl")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[REQUIRED_COLS].copy()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    for col in ["Pay Rate", "Bill Rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["Position", "County of Venue", "Industry"]:
        df[col] = df[col].astype(str).str.strip()

    df = df.dropna(
        subset=["Date", "Bill Rate", "Pay Rate", "Position", "Industry", "County of Venue"]
    )

    return df


st.set_page_config(page_title="Rate Analyzer", layout="wide")
st.title("Payroll Rate Analyzer (Local)")

st.markdown(
    """
Analyze payroll data by Date Range, Position, Industry, and County of Venue.
The app computes min/max/avg Bill Rate and Pay Rate and visualizes shift counts per Bill Rate.
"""
)

try:
    df = load_payroll_data(PAYROLL_PATH)
except (FileNotFoundError, ValueError) as exc:
    st.error(str(exc))
    st.stop()

st.success(f"Loaded {len(df):,} shifts.")

min_date = df["Date"].min()
max_date = df["Date"].max()

positions = sorted(df["Position"].dropna().unique().tolist())
industries = sorted(df["Industry"].dropna().unique().tolist())
counties = sorted(df["County of Venue"].dropna().unique().tolist())

with st.sidebar:
    st.header("Filters")

    start_date, end_date = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    position_sel = st.selectbox("Position", options=["(All)"] + positions, index=0)
    industry_sel = st.selectbox("Industry", options=["(All)"] + industries, index=0)
    county_sel = st.multiselect(
        "County of Venue (multi-select)", options=counties, default=[]
    )

    st.divider()
    st.subheader("Chart Options")
    round_bill_rate = st.checkbox("Round Bill Rate to cents (recommended)", value=True)
    max_bars = st.number_input(
        "Max bars to display (top by shift count)",
        min_value=5,
        max_value=200,
        value=50,
        step=5,
    )

filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)].copy()

if position_sel != "(All)":
    filtered = filtered[filtered["Position"] == position_sel]

if industry_sel != "(All)":
    filtered = filtered[filtered["Industry"] == industry_sel]

if county_sel:
    filtered = filtered[filtered["County of Venue"].isin(county_sel)]

st.subheader("Filtered Results")

if filtered.empty:
    st.warning("No rows match the current filters.")
    st.stop()

if round_bill_rate:
    filtered["Bill Rate"] = filtered["Bill Rate"].round(2)

bill_min = filtered["Bill Rate"].min()
bill_max = filtered["Bill Rate"].max()
bill_avg = filtered["Bill Rate"].mean()
pay_min = filtered["Pay Rate"].min()
pay_max = filtered["Pay Rate"].max()
pay_avg = filtered["Pay Rate"].mean()

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Filtered Shifts", f"{len(filtered):,}")
c2.metric("Bill Rate (Min)", f"{bill_min:,.2f}")
c3.metric("Bill Rate (Avg)", f"{bill_avg:,.2f}")
c4.metric("Bill Rate (Max)", f"{bill_max:,.2f}")
c5.metric("Pay Rate (Min)", f"{pay_min:,.2f}")
c6.metric("Pay Rate (Avg)", f"{pay_avg:,.2f}")
c7.metric("Pay Rate (Max)", f"{pay_max:,.2f}")

rate_counts = (
    filtered.groupby("Bill Rate", dropna=False)
    .size()
    .reset_index(name="Shift Count")
    .sort_values(["Bill Rate"], ascending=True)
)

st.markdown("### Shifts per Bill Rate")

rate_counts_for_chart = rate_counts.copy()
if len(rate_counts_for_chart) > int(max_bars):
    rate_counts_for_chart = (
        rate_counts_for_chart.sort_values("Shift Count", ascending=False)
        .head(int(max_bars))
        .sort_values("Bill Rate", ascending=True)
    )
    st.info(
        f"Showing top {int(max_bars)} bill rates by shift count (out of "
        f"{len(rate_counts):,} distinct rates) for chart readability."
    )

fig, ax = plt.subplots()
x_labels = rate_counts_for_chart["Bill Rate"].astype(str).tolist()
y_vals = rate_counts_for_chart["Shift Count"].tolist()

ax.bar(range(len(x_labels)), y_vals)
ax.set_xlabel("Bill Rate")
ax.set_ylabel("Number of Shifts")
ax.set_title("Shifts per Bill Rate")
ax.set_xticks(range(len(x_labels)))
ax.set_xticklabels(x_labels, rotation=45, ha="right")

st.pyplot(fig, clear_figure=True)

st.dataframe(rate_counts, use_container_width=True)

st.markdown("### Sample of Filtered Rows")
st.dataframe(filtered.head(200), use_container_width=True)

csv = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered Data (CSV)",
    data=csv,
    file_name="filtered_payroll_data.csv",
    mime="text/csv",
)

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_PATH = BASE_DIR / "payroll.xlsx"


@st.cache_data(show_spinner=False)
def load_payroll_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Payroll workbook not found at {path}. Please commit payroll.xlsx to the repo root."
        )

    df = pd.read_excel(path)
    if df.empty:
        raise ValueError("Payroll workbook is empty.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


st.set_page_config(page_title="Sales Payroll Analyzer", layout="wide")

st.title("Sales Payroll Analyzer")
st.markdown(
    "Analyze payroll data by date range, position, industry, and county of venue."
)

try:
    payroll_df = load_payroll_data(PAYROLL_PATH)
except (FileNotFoundError, ValueError) as exc:
    st.error(str(exc))
    st.stop()

valid_dates = payroll_df["Date"].dropna()
if valid_dates.empty:
    st.error("No valid dates found in the payroll data.")
    st.stop()

min_date = valid_dates.min().date()
max_date = valid_dates.max().date()

with st.sidebar:
    st.header("Filters")
    start_date, end_date = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    position_options = sorted(payroll_df["Position"].dropna().unique())
    industry_options = sorted(payroll_df["Industry"].dropna().unique())
    county_options = sorted(payroll_df["County of Venue"].dropna().unique())

    selected_positions = st.multiselect(
        "Position", position_options, default=position_options
    )
    selected_industries = st.multiselect(
        "Industry", industry_options, default=industry_options
    )
    selected_counties = st.multiselect(
        "County of Venue", county_options, default=county_options
    )

start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date)

filtered = payroll_df.copy()
filtered = filtered[filtered["Date"].between(start_ts, end_ts, inclusive="both")]
filtered = filtered[filtered["Position"].isin(selected_positions)]
filtered = filtered[filtered["Industry"].isin(selected_industries)]
filtered = filtered[filtered["County of Venue"].isin(selected_counties)]

st.subheader("Filtered summary")
st.caption(f"Records: {len(filtered):,}")

rate_df = filtered.copy()
rate_df["Bill Rate"] = to_numeric(rate_df["Bill Rate"])
rate_df["Pay Rate"] = to_numeric(rate_df["Pay Rate"])

bill_rate_stats = rate_df["Bill Rate"].agg(["min", "mean", "max"]).rename("Bill Rate")
pay_rate_stats = rate_df["Pay Rate"].agg(["min", "mean", "max"]).rename("Pay Rate")

stats_table = pd.concat([bill_rate_stats, pay_rate_stats], axis=1)
st.dataframe(
    stats_table.style.format("{:.2f}").set_caption("Rate statistics"),
    use_container_width=True,
)

st.subheader("Breakdown by Position, Industry, and County")

if filtered.empty:
    st.info("No records match the selected filters.")
else:
    grouped = (
        rate_df.groupby(["Position", "Industry", "County of Venue"], dropna=False)
        .agg(
            bill_rate_min=("Bill Rate", "min"),
            bill_rate_avg=("Bill Rate", "mean"),
            bill_rate_max=("Bill Rate", "max"),
            pay_rate_min=("Pay Rate", "min"),
            pay_rate_avg=("Pay Rate", "mean"),
            pay_rate_max=("Pay Rate", "max"),
            records=("Bill Rate", "count"),
        )
        .reset_index()
        .sort_values(["records", "bill_rate_avg"], ascending=[False, False])
    )

    st.dataframe(grouped, use_container_width=True)

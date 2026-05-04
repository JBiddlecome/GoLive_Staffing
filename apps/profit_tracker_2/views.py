import shutil
import tempfile
from pathlib import Path
from datetime import date

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional

router = APIRouter()
templates = Jinja2Templates(directory="templates")

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_PATH = BASE_DIR / "payroll.xlsx"


def _load_payroll() -> pd.DataFrame:
    if not PAYROLL_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail="payroll.xlsx not found in repository root.",
        )

    # Copy via shutil first so we can still read while Excel has the workbook open
    # (Excel holds an exclusive lock that blocks pandas' direct open()).
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        shutil.copyfile(PAYROLL_PATH, tmp_path)
        df = pd.read_excel(tmp_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read payroll.xlsx: {exc}")
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for col in ("Total Bill", "Gross Pay", "MSP rate", "WC rate"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


@router.get("", response_class=HTMLResponse)
async def profit_tracker_2_page(request: Request):
    today = date.today().isoformat()
    return templates.TemplateResponse(
        "apps/profit_tracker_2.html",
        {"request": request, "start_date": today, "end_date": today},
    )


class ProfitPayload(BaseModel):
    start_date: str
    end_date: str
    client_name: Optional[str] = None


@router.post("/api/data")
async def get_profit_data(payload: ProfitPayload):
    try:
        start = pd.to_datetime(payload.start_date)
        end = pd.to_datetime(payload.end_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format.")

    df = _load_payroll()
    mask = df["Date"].between(start, end, inclusive="both")
    df = df.loc[mask].copy()

    if df.empty:
        return JSONResponse({
            "total_bill": 0,
            "gross_pay": 0,
            "msp_fee": 0,
            "wc_fee": 0,
            "payroll_tax": 0,
            "other_work": 0,
            "profit": 0,
            "client_breakdown": [],
        })

    client_str = df["Client"].astype("string").str.strip()
    has_client = client_str.notna() & (client_str != "") & (client_str.str.lower() != "nan")

    other_work_sum = float(df.loc[~has_client, "Gross Pay"].sum())

    client_df = df.loc[has_client].copy()
    client_df["client_name"] = client_str[has_client]
    client_df["msp_fee"] = client_df["Total Bill"] * client_df["MSP rate"]
    client_df["wc_fee"] = client_df["Total Bill"] * client_df["WC rate"]

    total_bill_sum = float(client_df["Total Bill"].sum())
    gross_pay_sum = float(client_df["Gross Pay"].sum())
    msp_fee_sum = float(client_df["msp_fee"].sum())
    wc_fee_sum = float(client_df["wc_fee"].sum())

    payroll_tax = gross_pay_sum * 0.10
    profit = (
        total_bill_sum
        - gross_pay_sum
        - msp_fee_sum
        - wc_fee_sum
        - payroll_tax
        - other_work_sum
    )

    grouped = (
        client_df.groupby("client_name")[["Total Bill", "Gross Pay", "msp_fee", "wc_fee"]]
        .sum()
        .reset_index()
    )

    client_breakdown = []
    for _, r in grouped.iterrows():
        c_bill = float(r["Total Bill"])
        c_pay = float(r["Gross Pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_tax
        c_profit_pct = (c_profit / c_bill * 100) if c_bill else 0.0
        client_breakdown.append({
            "client_name": r["client_name"],
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2),
            "profit_pct": round(c_profit_pct, 1),
        })

    client_breakdown.sort(key=lambda x: x["total_bill"], reverse=True)

    return JSONResponse({
        "total_bill": round(total_bill_sum, 2),
        "gross_pay": round(gross_pay_sum, 2),
        "msp_fee": round(msp_fee_sum, 2),
        "wc_fee": round(wc_fee_sum, 2),
        "payroll_tax": round(payroll_tax, 2),
        "other_work": round(other_work_sum, 2),
        "profit": round(profit, 2),
        "client_breakdown": client_breakdown,
    })


@router.post("/api/weekly-client-data")
async def get_weekly_client_data(payload: ProfitPayload):
    if not payload.client_name:
        raise HTTPException(status_code=400, detail="Client name is required.")

    try:
        start = pd.to_datetime(payload.start_date)
        end = pd.to_datetime(payload.end_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format.")

    df = _load_payroll()
    
    # Filter by client first
    client_mask = df["Client"].astype("string").str.strip() == payload.client_name
    df = df.loc[client_mask].copy()
    
    if df.empty:
        return JSONResponse({"weeks": []})

    # Filter by date range
    mask = df["Date"].between(start, end, inclusive="both")
    df = df.loc[mask].copy()
    
    if df.empty:
        return JSONResponse({"weeks": []})

    # Calculate additional columns
    df["msp_fee"] = df["Total Bill"] * df["MSP rate"]
    df["wc_fee"] = df["Total Bill"] * df["WC rate"]
    
    # Group by week (Monday-Sunday)
    # W-SUN means week ending on Sunday. label='left' would label it by the Monday.
    # However, to ensure it starts on Monday, we use 'W-SUN' and ensure our start/end dates align if needed, 
    # but pandas resample handles the bins.
    
    # We want Monday labels.
    df.set_index("Date", inplace=True)
    weekly = df.resample("W-MON", label="left", closed="left")[["Total Bill", "Gross Pay", "msp_fee", "wc_fee"]].sum()
    
    weeks_data = []
    for dt, r in weekly.iterrows():
        c_bill = float(r["Total Bill"])
        c_pay = float(r["Gross Pay"])
        c_msp = float(r["msp_fee"])
        c_wc = float(r["wc_fee"])
        c_tax = c_pay * 0.10
        c_profit = c_bill - c_pay - c_msp - c_wc - c_tax
        c_profit_pct = (c_profit / c_bill * 100) if c_bill else 0.0
        
        weeks_data.append({
            "week_start": dt.strftime("%Y-%m-%d"),
            "total_bill": round(c_bill, 2),
            "gross_pay": round(c_pay, 2),
            "msp_fee": round(c_msp, 2),
            "wc_fee": round(c_wc, 2),
            "payroll_tax": round(c_tax, 2),
            "profit": round(c_profit, 2),
            "profit_pct": round(c_profit_pct, 1),
        })

    return JSONResponse({"weeks": weeks_data})

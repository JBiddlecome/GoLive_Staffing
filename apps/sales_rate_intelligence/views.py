from datetime import datetime
from typing import Optional, List
import pandas as pd
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import io
import math

from .database import get_raw_rate_data_async

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def sales_rate_intelligence_dashboard(request: Request):
    return templates.TemplateResponse("apps/sales_rate_intelligence.html", {"request": request})

def safe_round(val):
    if pd.isna(val) or math.isnan(val):
        return 0.0
    return round(float(val), 2)

@router.get("/api/benchmark")
async def get_benchmark_data(
    position_id: Optional[int] = None,
    county_id: Optional[int] = None,
    industry: Optional[str] = None,
    date_start: Optional[str] = None,  # YYYY-MM-DD
    date_end: Optional[str] = None,    # YYYY-MM-DD
    export_csv: Optional[bool] = False
):
    df = await get_raw_rate_data_async(date_start, date_end)
    
    if df.empty:
        return JSONResponse({"kpis": {}, "percentiles": {}, "comparables": [], "warning": "No data available."})
    
    # 1. Filter the raw data
    filtered_df = df.copy()
    
    if position_id is not None:
        filtered_df = filtered_df[filtered_df['position_id'] == position_id]
        
    if county_id is not None:
        filtered_df = filtered_df[filtered_df['county_id'] == county_id]
        
    if industry is not None and industry != "":
        filtered_df = filtered_df[filtered_df['industry_normalized'] == industry]
        
    # The SQL query natively handled the e.date >= ds AND e.date <= de logic, so we do not need pandas date masks!
        
    # 2. De-duplicate:
    # A client might have multiple effective configurations. 
    # Sort by 'record_created_at' descending so the active/latest rate is kept.
    # We group by client, position, and county to accurately show market volume per county where they threw shifts.
    filtered_df = filtered_df.sort_values(by=['record_created_at'], ascending=[False], na_position='last')
    dedup_df = filtered_df.drop_duplicates(subset=['client_id', 'position_id', 'county_id'])
    
    if dedup_df.empty:
        return JSONResponse({"kpis": {}, "percentiles": {}, "comparables": [], "warning": "No matching rate records found."})
        
    # 3. Calculate KPIs
    kpis = {
        "avg_bill_rate": safe_round(dedup_df['bill_rate'].mean()),
        "avg_pay_rate": safe_round(dedup_df['pay_rate'].mean()),
        "avg_spread": safe_round(dedup_df['spread_dollars'].mean()),
        "avg_markup_pct": safe_round(dedup_df['markup_percent'].mean()),
        "sample_clients": int(dedup_df['client_id'].nunique()),
        "sample_records": int(len(dedup_df))
    }
    
    warning = None
    if kpis['sample_clients'] < 5 or kpis['sample_records'] < 10:
        warning = "Low Confidence: Sample size is extremely small. Data may not be representative."
        if kpis['sample_clients'] <= 2:
            warning = "Very Low Confidence: Only 1-2 accounts match. These benchmarks are highly speculative."
            
    # 4. Benchmarks (Percentiles)
    percentiles = {
        "bill_min": safe_round(dedup_df['bill_rate'].min()),
        "bill_25": safe_round(dedup_df['bill_rate'].quantile(0.25)),
        "bill_median": safe_round(dedup_df['bill_rate'].median()),
        "bill_75": safe_round(dedup_df['bill_rate'].quantile(0.75)),
        "bill_max": safe_round(dedup_df['bill_rate'].max()),
        
        "pay_min": safe_round(dedup_df['pay_rate'].min()),
        "pay_25": safe_round(dedup_df['pay_rate'].quantile(0.25)),
        "pay_median": safe_round(dedup_df['pay_rate'].median()),
        "pay_75": safe_round(dedup_df['pay_rate'].quantile(0.75)),
        "pay_max": safe_round(dedup_df['pay_rate'].max()),
    }
    
    # 5. Comparables list
    comparables = dedup_df[[
        'client_name', 'industry_normalized', 'county_name', 'position_name', 
        'bill_rate', 'pay_rate', 'surcharge', 'start_date', 'end_date',
        'sales_executive_name', 'bundle', 'won_date', 'record_created_at'
    ]].copy()
    
    for c in ['start_date', 'end_date', 'won_date', 'record_created_at']:
        comparables[c] = comparables[c].astype(str).replace('NaT', '').replace('None', '')
        
    comparables = comparables.sort_values(by=['bill_rate', 'client_name'], ascending=[False, True]).fillna('')
    comp_list = comparables.to_dict(orient='records')
    
    if export_csv:
        stream = io.StringIO()
        comparables.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=sales_benchmark_export.csv"
        return response

    return JSONResponse({
        "kpis": kpis,
        "percentiles": percentiles,
        "comparables": comp_list,
        "warning": warning
    })

@router.get("/api/filters")
async def get_filter_options():
    df = await get_raw_rate_data_async()
    if df.empty:
        return JSONResponse({"counties": [], "positions": [], "industries": []})
        
    counties_df = df[['county_id', 'county_name']].dropna().drop_duplicates().sort_values('county_name')
    counties = counties_df.to_dict(orient='records')
    
    pos_df = df[['position_id', 'position_name']].dropna().drop_duplicates().sort_values('position_name')
    positions = pos_df.to_dict(orient='records')
    
    inds = sorted([i for i in df['industry_normalized'].unique() if i and i != 'Unknown'])
    
    return JSONResponse({
        "counties": counties,
        "positions": positions,
        "industries": inds
    })

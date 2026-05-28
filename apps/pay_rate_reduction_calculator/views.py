import os
import pandas as pd
import numpy as np
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Dict, Optional

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Resolve path dynamically to support both local Windows and live Render server
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXCEL_PATH = os.path.join(BASE_DIR, "shifts_report_april_may_2026.xlsx")

LOCAL_FALLBACK = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\shifts_report_april_may_2026.xlsx"
if not os.path.exists(EXCEL_PATH) and os.path.exists(LOCAL_FALLBACK):
    EXCEL_PATH = LOCAL_FALLBACK

import shutil
import uuid

def read_excel_robust(file_path: str) -> pd.DataFrame:
    """Reads Excel file safely even if locked by another program on Windows."""
    temp_dir = os.path.join(os.path.dirname(file_path), "tmp")
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = os.path.join(temp_dir, f"temp_{uuid.uuid4().hex}_{os.path.basename(file_path)}")
    try:
        shutil.copy2(file_path, temp_path)
        return pd.read_excel(temp_path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

class RecalculateRequest(BaseModel):
    custom_rates: Dict[str, Optional[float]]

@router.get("", response_class=HTMLResponse)
async def pay_rate_reduction_calculator_page(request: Request):
    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(
            status_code=404,
            detail=f"Shift report Excel file not found at: {EXCEL_PATH}. Please generate it first."
        )
        
    try:
        df = read_excel_robust(EXCEL_PATH)
        
        # Replace NaNs in crucial columns
        df['Original Rate'] = df['Original Rate'].fillna(0.0).astype(float)
        df['Hours Worked'] = df['Hours Worked'].fillna(0.0).astype(float)
        df['Minimum Wage'] = df['Minimum Wage'].fillna(0.0).astype(float)
        df['Less $2 Rate'] = df['Less $2 Rate'].fillna(df['Original Rate']).astype(float)
        df['Rate Lock'] = df['Rate Lock'].fillna('No').astype(str).str.strip()
        df['County'] = df['County'].fillna('Unknown County').astype(str).str.strip()
        df['Position Title'] = df['Position Title'].fillna('Unknown Position').astype(str).str.strip()
        
        # 1. Calculate Baselines
        total_original_paid = float((df['Hours Worked'] * df['Original Rate']).sum())
        total_less_2_paid = float((df['Hours Worked'] * df['Less $2 Rate']).sum())
        savings_less_2 = total_original_paid - total_less_2_paid
        savings_pct_less_2 = (savings_less_2 / total_original_paid * 100) if total_original_paid > 0 else 0.0
        
        # 2. Group by County and Position Title for inputs
        # We want to exclude rows where County or Position is missing or default Unknown
        valid_df = df[
            (df['County'] != 'Unknown County') & 
            (df['Position Title'] != 'Unknown Position')
        ]
        
        grouped = valid_df.groupby(['County', 'Position Title']).agg(
            shifts_count=('Shift Employee ID', 'count'),
            avg_orig_rate=('Original Rate', 'mean'),
            min_wage=('Minimum Wage', 'min'), # Min to get the lowest min wage floor for the UI input
            max_min_wage=('Minimum Wage', 'max'), # Max to check if there is a range of min wages
            locked_count=('Rate Lock', lambda x: (x.str.lower() == 'yes').sum())
        ).reset_index()
        
        # Convert to dictionary sorted by County
        counties_data = {}
        for _, row in grouped.iterrows():
            county = row['County']
            if county not in counties_data:
                counties_data[county] = []
                
            counties_data[county].append({
                "position": row['Position Title'],
                "shifts": int(row['shifts_count']),
                "avg_orig_rate": round(float(row['avg_orig_rate']), 2),
                "min_wage": round(float(row['min_wage']), 2),
                "max_min_wage": round(float(row['max_min_wage']), 2),
                "locked_shifts": int(row['locked_count'])
            })
            
        # Sort positions within each county by shift count descending
        for county in counties_data:
            counties_data[county].sort(key=lambda x: x['shifts'], reverse=True)
            
        # Sort counties by total shifts descending
        sorted_counties = sorted(
            counties_data.items(), 
            key=lambda x: sum(p['shifts'] for p in x[1]), 
            reverse=True
        )
        
        return templates.TemplateResponse(
            "apps/pay_rate_reduction_calculator.html",
            {
                "request": request,
                "total_original_paid": total_original_paid,
                "total_less_2_paid": total_less_2_paid,
                "savings_less_2": savings_less_2,
                "savings_pct_less_2": savings_pct_less_2,
                "counties": sorted_counties,
                "total_shifts": len(df)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shift report: {str(e)}")

@router.post("/calculate")
async def calculate_custom_rates(payload: RecalculateRequest):
    if not os.path.exists(EXCEL_PATH):
        return JSONResponse(
            status_code=404,
            content={"message": "Shift report Excel file not found."}
        )
        
    try:
        df = read_excel_robust(EXCEL_PATH)
        
        df['Original Rate'] = df['Original Rate'].fillna(0.0).astype(float)
        df['Hours Worked'] = df['Hours Worked'].fillna(0.0).astype(float)
        df['Minimum Wage'] = df['Minimum Wage'].fillna(0.0).astype(float)
        df['Rate Lock'] = df['Rate Lock'].fillna('No').astype(str).str.strip()
        df['County'] = df['County'].fillna('Unknown County').astype(str).str.strip()
        df['Position Title'] = df['Position Title'].fillna('Unknown Position').astype(str).str.strip()
        
        custom_rates_map = payload.custom_rates
        
        total_original_paid = 0.0
        total_custom_paid = 0.0
        
        # Process rows in a high-speed loop
        for _, row in df.iterrows():
            hours = float(row['Hours Worked'])
            orig_rate = float(row['Original Rate'])
            min_wage = float(row['Minimum Wage'])
            lock = str(row['Rate Lock']).lower() == 'yes'
            county = str(row['County'])
            pos = str(row['Position Title'])
            
            key = f"{county}|{pos}"
            
            total_original_paid += hours * orig_rate
            
            # Apply Custom rate logic
            if key in custom_rates_map and custom_rates_map[key] is not None:
                custom_rate = custom_rates_map[key]
                if lock:
                    # Locked rows CANNOT have their rates changed
                    final_rate = orig_rate
                else:
                    # No rate can fall below Minimum Wage
                    final_rate = max(custom_rate, min_wage)
            else:
                final_rate = orig_rate
                
            total_custom_paid += hours * final_rate
            
        savings = total_original_paid - total_custom_paid
        savings_pct = (savings / total_original_paid * 100) if total_original_paid > 0 else 0.0
        
        return {
            "total_original_paid": total_original_paid,
            "total_custom_paid": total_custom_paid,
            "savings": savings,
            "savings_pct": savings_pct
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"message": f"Error running calculations: {str(e)}"}
        )

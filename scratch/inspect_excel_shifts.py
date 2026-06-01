import os
import pandas as pd
import tempfile
import shutil

excel_path = r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\shifts_report_april_may_2026.xlsx"
temp_dir = tempfile.gettempdir()
temp_path = os.path.join(temp_dir, "temp_shifts_report.xlsx")

try:
    shutil.copy2(excel_path, temp_path)
    df = pd.read_excel(temp_path)
    print("Excel columns:", df.columns.tolist())
    print("\nShape:", df.shape)
    print("\nUnique Client Names:")
    if "Client Name" in df.columns:
        print(df["Client Name"].unique()[:20])
    elif "Client" in df.columns:
        print(df["Client"].unique()[:20])
    else:
        # print some row keys to see what is there
        print(df.head(2))
    
    print("\nUnique MSP columns:")
    msp_cols = [c for c in df.columns if "msp" in c.lower()]
    print("MSP related columns:", msp_cols)
    for col in msp_cols:
        print(f"Unique values in {col}:", df[col].unique())
        
except Exception as e:
    print("Error:", e)
finally:
    if os.path.exists(temp_path):
        os.remove(temp_path)

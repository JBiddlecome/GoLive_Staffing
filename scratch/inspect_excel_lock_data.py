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
    compass_df = df[df["MSP"] == "Compass"]
    print("Total Compass shifts:", len(compass_df))
    print("Locked Compass shifts:", len(compass_df[compass_df["Rate Lock"] == "Yes"]))
    print("Locked clients:", compass_df[compass_df["Rate Lock"] == "Yes"]["Client Name"].unique())
except Exception as e:
    print("Error:", e)
finally:
    if os.path.exists(temp_path):
        os.remove(temp_path)

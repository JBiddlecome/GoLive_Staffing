import pandas as pd
import shutil
import tempfile
import os

excel_path = r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\compass_position_ids.xlsx"
temp_dir = tempfile.gettempdir()
temp_excel_path = os.path.join(temp_dir, "temp_compass_position_ids.xlsx")

try:
    shutil.copy2(excel_path, temp_excel_path)
    print(f"Successfully copied locked file to {temp_excel_path}")
    df = pd.read_excel(temp_excel_path)
    print("Columns in Excel file:", df.columns)
    print("First 10 rows:")
    print(df.head(10))
    print("Total rows:", len(df))
    # clean up
    os.remove(temp_excel_path)
except Exception as e:
    print(f"Error copying/reading file: {e}")

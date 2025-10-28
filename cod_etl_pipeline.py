# cod_etl_pipeline.py
# FULL BSS-COMPLIANT ETL + SQLITE LOAD

import pandas as pd
import os
import glob
from datetime import datetime
import hashlib
import sqlite3

print(f"[{datetime.now()}] Starting COD Store-Level ETL Pipeline...")

# === CONFIG ===
BASE_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(BASE_DIR, "input_files")
OUTPUT_DIR = os.path.join(BASE_DIR, "harmonized_output")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
REPORT_DIR = os.path.join(BASE_DIR, "reports")
DB_PATH = os.path.join(BASE_DIR, "cod_store_data.db")  # DB will be here

for d in [INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR, REPORT_DIR]:
    os.makedirs(d, exist_ok=True)

# === Delta Load: MD5 ===
def get_file_hash(filepath):
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

processed_files = {}
for archive_path in glob.glob(os.path.join(ARCHIVE_DIR, "*.csv")):
    filename = os.path.basename(archive_path)
    processed_files[filename] = get_file_hash(archive_path)

# === Find New Files ===
pattern = os.path.join(INPUT_DIR, "*_??????_??????_*.csv")
new_files = []
for file_path in glob.glob(pattern):
    filename = os.path.basename(file_path)
    if filename in processed_files:
        print(f"Skipping {filename} (already processed)")
        continue
    new_files.append(file_path)

if not new_files:
    print("No new files.")
    exit()

# === Process Files ===
validation_report = []
for file_path in new_files:
    print(f"Processing: {file_path}")
    df = pd.read_csv(file_path)
    filename = os.path.basename(file_path).replace('.csv', '')

    # Top Line Validation
    required = ['Store_ID', 'Store_Name', 'City', 'Volume', 'Value']
    missing = [c for c in required if c not in df.columns]
    validation_report.append({
        'File': filename,
        'Rows': len(df),
        'Missing_Cols': ', '.join(missing) if missing else 'None',
        'Status': 'FAILED' if missing else 'PASSED'
    })
    if missing:
        print(f"FAILED: {missing}")
        continue

    # Parse filename
    parts = filename.split('_')
    country = parts[0][:2]
    source = parts[0][2:5]
    df['Country'] = country
    df['Source'] = source
    df['Unique_Store_ID'] = df['Store_ID'].astype(str) + "_" + df.get('Banner', 'N/A')
    df['Load_Timestamp'] = datetime.now()
    df['Period_Start'] = parts[1]
    df['Period_End'] = parts[2]

    # Save CDM
    output_file = os.path.join(OUTPUT_DIR, f"CDM_{filename}.csv")
    df.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")

    # Archive
    os.replace(file_path, os.path.join(ARCHIVE_DIR, os.path.basename(file_path)))

# === Validation Report ===
report_df = pd.DataFrame(validation_report)
report_path = os.path.join(REPORT_DIR, f"Validation_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
report_df.to_excel(report_path, index=False)
print(f"Report: {report_path}")

# === LOAD INTO SQLITE ===
print("Loading into SQLite...")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Drop + Create Table
cursor.execute("DROP TABLE IF EXISTS store_offtake")
cursor.execute("""
CREATE TABLE store_offtake (
    Store_ID TEXT,
    Store_Name TEXT,
    City TEXT,
    Volume REAL,
    Value REAL,
    Banner TEXT,
    Street TEXT,
    Post_Code TEXT,
    Key_Account TEXT,
    Country TEXT,
    Source TEXT,
    Unique_Store_ID TEXT,
    Load_Timestamp TEXT,
    Period_Start TEXT,
    Period_End TEXT
)
""")

# Load all CDM files
for file in glob.glob(os.path.join(OUTPUT_DIR, "CDM_*.csv")):
    df = pd.read_csv(file)
    df.to_sql('store_offtake', conn, if_exists='append', index=False)
    print(f"Loaded: {os.path.basename(file)}")

conn.commit()
conn.close()

print(f"\nSUCCESS: SQLite DB created at:\n   {DB_PATH}")
print("   Table: store_offtake")
print("ETL + DB Load Complete!")
import os
import sqlite3
import zipfile
import csv
from tqdm import tqdm

# Hardcoded paths
ZIP_FOLDER = "data_zip"
DB_FILE = "forex_data.db"

def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print("Removed existing forex_data.db")
    return sqlite3.connect(DB_FILE)

def create_table(conn, table_name):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            UNIQUE(date, time)
        )
    """)

def process_zip_file(zip_path, conn):
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for inner_name in z.namelist():
                if not inner_name.endswith(".txt"):
                    continue

                # Parse pair + timeframe
                parts = os.path.basename(inner_name).split("_")
                if len(parts) < 4:
                    print(f"Skipping {inner_name}: unexpected name format")
                    continue
                    
                pair = parts[2]
                timeframe = parts[3].split('.')[0]  # Remove .txt extension
                table_name = f"{pair}_{timeframe}"
                create_table(conn, table_name)

                # Read entire file to handle headers
                with z.open(inner_name) as f:
                    content = f.read().decode('utf-8').splitlines()
                
                reader = csv.reader(content, delimiter=',')
                batch = []
                
                for i, row in enumerate(reader):
                    # Skip empty rows
                    if not row:
                        continue
                        
                    # Skip rows with only empty strings
                    if all(cell.strip() == '' for cell in row):
                        continue
                        
                    # Skip header rows
                    if any('#' in cell for cell in row) or any('Time' in cell for cell in row):
                        continue
                        
                    # Skip rows with insufficient data
                    if len(row) < 7:
                        # Only log if it's not an empty line
                        if any(cell.strip() for cell in row):
                            print(f"Row {i+1} in {inner_name} has {len(row)} columns (expected 7). Skipping.")
                        continue

                    try:
                        # Parse and convert date format (YYYY.MM.DD → YYYYMMDD)
                        date_str = row[0].strip().replace('.', '')
                        if len(date_str) != 8:
                            raise ValueError(f"Invalid date format: {row[0]}")

                        # Parse and convert time format (HH:MM → HHMM00)
                        time_str = row[1].strip()
                        if ':' in time_str:
                            time_str = time_str.replace(':', '') + '00'
                        if len(time_str) != 6:
                            raise ValueError(f"Invalid time format: {row[1]}")
                            
                        # Parse numeric values
                        open_val = float(row[2])
                        high_val = float(row[3])
                        low_val = float(row[4])
                        close_val = float(row[5])
                        volume = int(float(row[6]))  # Handle float volume
                        
                        batch.append((date_str, time_str, open_val, high_val, 
                                     low_val, close_val, volume))
                        
                    except (ValueError, IndexError) as e:
                        print(f"Error row {i+1} in {inner_name}: {e} | Row: {row}")
                        continue
                    
                    # Batch insert
                    if len(batch) >= 10000:
                        conn.executemany(
                            f"INSERT OR IGNORE INTO {table_name} "
                            "(date, time, open, high, low, close, volume) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
                        conn.commit()
                        batch = []
                
                # Final batch insert
                if batch:
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {table_name} "
                        "(date, time, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
                    conn.commit()

    except zipfile.BadZipFile:
        print(f"Corrupted zip file: {zip_path}")

def main():
    conn = init_db()
    zip_files = sorted([
        f for f in os.listdir(ZIP_FOLDER) 
        if f.endswith(".zip")
    ])

    print(f"Processing {len(zip_files)} zip files...")
    for zip_file in tqdm(zip_files, desc="Processing", unit="file"):
        zip_path = os.path.join(ZIP_FOLDER, zip_file)
        print(f"\nProcessing {zip_file}...")
        process_zip_file(zip_path, conn)
        
    conn.close()
    print("✅ Done! Database created at", DB_FILE)

if __name__ == "__main__":
    main()
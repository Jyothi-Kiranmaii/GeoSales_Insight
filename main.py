import sqlite3
import requests
import multiprocessing
import pandas as pd
import logging
import time
import ipaddress
from multiprocessing import Pool, cpu_count, freeze_support
from openpyxl import load_workbook

# Constants for file paths and configuration
API_KEY = "5e6abef4b6b223"
ORDERS_CSV = r"C:\Users\jyothi\Downloads\task\orders_file.csv"
IPS_CSV = r"C:\Users\jyothi\Downloads\task\ip_addresses.csv"
DB_FILE = r"C:\Users\jyothi\Downloads\task\orders_db.sqlite"
EXPORT_CSV = r"C:\Users\jyothi\Downloads\task\updated_orders.csv"

MAX_IPS = 100000
BATCH_SIZE = 1000
NUM_PROCESSES = cpu_count()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Create and return a new DB connection
def get_db_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

# Create database tables if they don't already exist
def create_tables():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Orders table
        cursor.execute('''CREATE TABLE IF NOT EXISTS orders (
            order_number TEXT PRIMARY KEY,
            date TEXT,
            city TEXT,
            state TEXT,
            Zip TEXT,
            "$ sale" TEXT,
            ip_address TEXT
        )''')
        # IP location data table
        cursor.execute('''CREATE TABLE IF NOT EXISTS ip_data (
            ip_address TEXT PRIMARY KEY,
            city TEXT,
            state TEXT,
            zip_code TEXT
        )''')
        # Indexes for faster lookup
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_city ON ip_data(city)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip_null ON ip_data(ip_address) WHERE city IS NULL OR city = ''")
        conn.commit()

# Validate IP address format
def validate_ip(ip):
    try:
        if pd.isna(ip):
            return None
        return str(ipaddress.ip_address(str(ip).strip()))
    except ValueError:
        return None

# Load IPs from CSV into the database (only new IPs)
def load_ip_data(file_path):
    df = pd.read_csv(file_path, dtype=str)
    df["ip_address"] = df["ip_address"].astype(str).apply(validate_ip)
    df = df.dropna(subset=["ip_address"])

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ip_address FROM ip_data")
        existing_ips = set(row[0] for row in cursor.fetchall())
        new_ips = [(ip,) for ip in df["ip_address"] if ip not in existing_ips]
        cursor.executemany("INSERT OR IGNORE INTO ip_data (ip_address) VALUES (?)", new_ips)
        conn.commit()
    logging.info(f"✅ Loaded {len(new_ips)} new IP addresses.")

# Fetch location data for a batch of IPs using ipinfo.io API
def fetch_bulk_ip_data(ips):
    url = f"https://ipinfo.io/batch?token={API_KEY}"
    try:
        response = requests.post(url, json=ips, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.warning(f"Error fetching IPs in bulk: {e}")
        return {}

# Merge IP addresses from the ip_addresses.csv into orders table
def merge_ips_into_orders():
    logging.info("🔄 Merging IP addresses into orders...")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        df = pd.read_csv(IPS_CSV, dtype=str)
        df["ip_address"] = df["ip_address"].astype(str).apply(validate_ip)
        df = df.dropna(subset=["ip_address"])
        df = df.drop_duplicates(subset="order_number")

        # Create a temporary table for bulk update
        cursor.execute("DROP TABLE IF EXISTS temp_ip_orders")
        cursor.execute('''CREATE TEMP TABLE temp_ip_orders (
            order_number TEXT PRIMARY KEY,
            ip_address TEXT
        )''')
        cursor.executemany('''INSERT INTO temp_ip_orders (order_number, ip_address)
                              VALUES (?, ?)''', df[["order_number", "ip_address"]].values.tolist())

        # Update orders with IPs
        cursor.execute('''UPDATE orders SET ip_address = (
            SELECT ip_address FROM temp_ip_orders WHERE temp_ip_orders.order_number = orders.order_number
        ) WHERE order_number IN (SELECT order_number FROM temp_ip_orders)
        ''')
        conn.commit()
    logging.info("✅ IPs merged into orders successfully.")

# Fetch missing location data in parallel and update ip_data table
def update_ip_data():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ip_address FROM ip_data WHERE city IS NULL OR city = ''")
        ip_list = [row[0] for row in cursor.fetchall()]

    logging.info(f"🚀 Processing {len(ip_list)} IPs using {multiprocessing.cpu_count()} processes...")

    # Batch the IPs for API requests
    ip_batches = [ip_list[i:i + BATCH_SIZE] for i in range(0, len(ip_list), BATCH_SIZE)]

    with Pool(processes=min(8, multiprocessing.cpu_count())) as pool:
        bulk_results = pool.map(fetch_bulk_ip_data, ip_batches)

    results = []
    for batch_data in bulk_results:
        for ip, details in batch_data.items():
            if isinstance(details, dict):  # Check for valid response
                city = details.get("city", "")
                state = details.get("region", "")
                zip_code = details.get("postal", "")
                if city:
                    results.append((city, state, zip_code, ip))
            else:
                logging.warning(f"⚠️ Skipping IP {ip} due to unexpected response: {details}")

    # Update ip_data table with fetched location info
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany("UPDATE ip_data SET city=?, state=?, zip_code=? WHERE ip_address=?", results)
        conn.commit()

    logging.info(f"✅ Updated {len(results)} IPs successfully.")

# Load orders data from CSV into database
def load_orders_data(file_path):
    df = pd.read_csv(file_path, dtype=str).fillna("")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute('''INSERT OR IGNORE INTO orders (order_number, date, city, state, Zip, "$ sale", ip_address)
                              VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                row.get("order_number", ""), row.get("date", ""), row.get("city", ""),
                row.get("state", ""), row.get("Zip", ""), row.get("$ sale", ""),
                row.get("ip_address", "")
            ))
        conn.commit()
    logging.info("✅ Orders data loaded successfully.")

# Update orders table with city/state/zip from ip_data table
def update_orders_table():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''UPDATE orders SET city = (
            SELECT ip_data.city FROM ip_data WHERE ip_data.ip_address = orders.ip_address
        ), state = (
            SELECT ip_data.state FROM ip_data WHERE ip_data.ip_address = orders.ip_address
        ), Zip = (
            SELECT ip_data.zip_code FROM ip_data WHERE ip_data.ip_address = orders.ip_address
        ) WHERE ip_address IS NOT NULL''')
        conn.commit()
    logging.info("✅ Orders table updated with location details from ip_data.")

# Export final updated orders table to CSV
def export_updated_data():
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM orders", conn)
    df.to_csv(EXPORT_CSV, index=False)
    logging.info(f"📁 Exported updated orders data to {EXPORT_CSV}")

# Ensure 'ip_address' column exists in orders table
def alter_orders_table():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(orders)")
        columns = [col[1] for col in cursor.fetchall()]
        if "ip_address" not in columns:
            cursor.execute("ALTER TABLE orders ADD COLUMN ip_address TEXT")
            conn.commit()
            logging.info("✅ Added 'ip_address' column to 'orders' table.")

# Generate quarterly sales report for a specific state and year
def generate_sales_report(state: str, year: int):
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM orders", conn)

    # Clean and filter data
    df = df[df["city"].notna() & (df["city"] != "")]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["$ sale"] = pd.to_numeric(df["$ sale"].str.replace(r"[^\d.]", "", regex=True), errors='coerce').fillna(0)
    df = df[(df["state"] == state) & (df["date"].dt.year == year)]
    df["quarter"] = df["date"].dt.quarter

    # Aggregate and pivot sales data
    grouped = df.groupby(["city", "quarter"])["$ sale"].sum().reset_index()
    pivot = grouped.pivot(index="city", columns="quarter", values="$ sale").fillna(0)
    pivot.columns = [f"Q{int(col)}" for col in pivot.columns]
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values(by="Total", ascending=False).reset_index()

    if df.empty:
        print("❌ No data found for the given state and year.")
        return

    # Write report to Excel
    output_file = f"{state}_state_sales_report_{year}_generated.xlsx"
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        pivot.to_excel(writer, index=False, sheet_name="SalesReport")

    print(f"✅ Excel report generated (no chart): {output_file}")

# Main workflow to orchestrate all steps
def main():
    create_tables()
    alter_orders_table()
    load_orders_data(ORDERS_CSV)
    merge_ips_into_orders()
    load_ip_data(IPS_CSV)
    update_ip_data()
    update_orders_table()
    # export_updated_data()
    generate_sales_report("Ontario", 2024)

# Entry point for multiprocessing compatibility on Windows
if __name__ == "__main__":
    freeze_support()
    main()

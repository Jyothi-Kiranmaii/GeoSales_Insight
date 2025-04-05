# 📊 GeoSales_Insight

**Enrich sales data with IP geolocation and generate state-wise Excel reports using Python.**

A Python-based data enrichment and reporting tool that transforms raw sales orders by enriching them with IP geolocation data and generating state-wise quarterly sales reports.

---

## 🧾 Order Enrichment and Sales Report Generator

This Python script processes customer orders by enriching them with geolocation data using the [ipinfo.io](https://ipinfo.io/) API. It then generates quarterly sales reports for a specified state and year.

---

## 🚀 Features

- Enrich orders with city, state, and ZIP code based on customer IP addresses
- Efficient IP enrichment with planned multiprocessing support
- Avoids redundant processing using a local SQLite database
- Aggregates sales data by city and quarter
- Exports results to a well-structured Excel report

---

## 📁 Input Files

- **`orders_file.csv`**: Contains raw order data (including order number, sale amount, and more)
- **`ip_addresses.csv`**: Contains a mapping of order numbers to customer IP addresses

---

## 🗃️ Database Schema

### SQLite Database: `orders.db`

#### `orders` Table

| Column       | Type  | Description                    |
|--------------|-------|--------------------------------|
| order_number | TEXT  | Unique ID of the order         |
| date         | TEXT  | Date of the order              |
| city         | TEXT  | Customer city (from IP)        |
| state        | TEXT  | Customer state (from IP)       |
| Zip          | TEXT  | Customer ZIP code (from IP)    |
| $ sale       | TEXT  | Sale amount                    |
| ip_address   | TEXT  | IP address of the customer     |

#### `ip_data` Table

| Column     | Type  | Description                   |
|------------|-------|-------------------------------|
| ip_address | TEXT  | Customer IP address (unique)  |
| city       | TEXT  | City (from IP geolocation)    |
| state      | TEXT  | State (from IP geolocation)   |
| zip_code   | TEXT  | ZIP Code (from IP geolocation)|

---

## 🧩 Function Breakdown

- ✅ **`create_tables()`**: Creates the necessary tables in the SQLite database.
- 🔧 **`alter_orders_table()`**: Ensures the `ip_address` column exists in the `orders` table.
- 🔍 **`validate_ip(ip)`**: Validates and normalizes IP addresses using Python’s `ipaddress` module.
- 📥 **`load_orders_data(file_path)`**: Loads order data from CSV into the database.
- 📥 **`load_ip_data(file_path)`**: Loads and inserts unique IPs into the `ip_data` table.
- 🔗 **`merge_ips_into_orders()`**: Associates IPs with orders based on `order_number`.
- 🌍 **`update_ip_data()`**: Fetches missing geolocation data from the API.
- ♻️ **`update_orders_table()`**: Updates orders with enriched geolocation info.
- 📊 **`generate_sales_report(state, year)`**: Creates a quarterly sales report and exports to Excel.

---

## 🧵 Workflow Overview

1. Initialize database tables
2. Add missing columns if needed
3. Load order data
4. Associate IPs with orders
5. Load and store new IPs
6. Enrich unknown IPs with location data
7. Update enriched orders
8. Generate report for **Ontario (2024)**

---

## 🛡️ Safeguards

- Skips duplicates with `INSERT OR IGNORE`
- IP address validation before insertion
- Avoids reprocessing known IPs
- Updates only empty fields

---

## 📤 Output

- **Excel File**: `Ontario_state_sales_report_2024_generated.xlsx`
- **Sheet Name**: `Ontario_state_sales_report_2024_generated`
- **Layout**:
  - Rows: Cities  
  - Columns: Quarters (`Q1` to `Q4`)  
  - Cells: Total sales per city/quarter

---

## 🧰 Requirements

- Python 3.8+
- Dependencies:
  ```bash
  pip install pandas openpyxl requests

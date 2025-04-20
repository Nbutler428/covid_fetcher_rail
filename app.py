import os
import requests
import psycopg2
from datetime import datetime

# Set environment variables
DATABASE_URL = os.environ['DATABASE_URL']
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')

# Define API URL
url = (
    "https://data.cityofnewyork.us/resource/rc75-m7u3.json?"
    "$where=date_of_interest>'2025-03-31T00:00:00.000'&$limit=1000"
)
print(f"📡 Fetching data from: {url}")

# Fetch the data
response = requests.get(url)
data = response.json()

if not data:
    print("⚠️ No data returned after the specified date.")
    exit(0)

# Extract column names
columns = list(data[0].keys())
columns_sql = ', '.join(columns)
placeholders = ', '.join(['%s'] * len(columns))

# Insert data into PostgreSQL
try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {', '.join([f"{col} TEXT" for col in columns])}
        );
    """)

    # Insert each row
    for row in data:
        values = [row.get(col, '') for col in columns]
        cur.execute(
            f"INSERT INTO {TABLE_NAME} ({columns_sql}) VALUES ({placeholders})",
            values
        )

    conn.commit()
    print("✅ Data inserted successfully.")

except Exception as e:
    print("❌ An error occurred during database interaction:")
    print(e)

finally:
    if 'cur' in locals(): cur.close()
    if 'conn' in locals(): conn.close()

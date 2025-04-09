import os
import requests
import psycopg2
from datetime import datetime

# Load environment variables
DATABASE_URL = os.environ['DATABASE_URL']
START_DATE = os.environ.get('START_DATE', datetime.now().strftime('%Y-%m-%d'))
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')

# Build the API URL
url = f"https://data.cityofnewyork.us/resource/rc75-m7u3.json"
print(f"Fetching data from: {url}")  # Debug output

# Fetch the data
response = requests.get(url)
data = response.json()

# Check for empty data
if not data:
    print("No data returned from the API. Please check your START_DATE or the API URL.")
    exit(1)

# Extract column names from the first record
columns = list(data[0].keys())
columns_sql = ', '.join(columns)
placeholders = ', '.join(['%s'] * len(columns))

# Connect to PostgreSQL and insert data
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Create table if it doesn't exist
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {', '.join([f"{col} TEXT" for col in columns])}
    );
""")

# Insert rows
for row in data:
    values = [row.get(col, '') for col in columns]
    cur.execute(
        f"INSERT INTO {TABLE_NAME} ({columns_sql}) VALUES ({placeholders})",
        values
    )

# Finalize
conn.commit()
cur.close()
conn.close()
print("Data inserted successfully.")

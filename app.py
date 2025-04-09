import os
import requests
import psycopg2
from datetime import datetime

# Load environment variables
DATABASE_URL = os.environ['DATABASE_URL']
START_DATE = os.environ.get('START_DATE', datetime.now().strftime('%Y-%m-%d'))
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')

# Build the API URL
url = f"https://data.cityofnewyork.us/resource/rc75-m7u3.json?$where=date_of_interest>'{START_DATE}'&$limit=1000"
response = requests.get(url)
data = response.json()

# Extract column names from the first record
columns = list(data[0].keys())
columns_sql = ', '.join(columns)
placeholders = ', '.join(['%s'] * len(columns))

# Connect to PostgreSQL and insert data
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {', '.join([f"{col} TEXT" for col in columns])}
    );
""")

for row in data:
    values = [row.get(col, '') for col in columns]
    cur.execute(
        f"INSERT INTO {TABLE_NAME} ({columns_sql}) VALUES ({placeholders})",
        values
    )

conn.commit()
cur.close()
conn.close()
print("Data inserted successfully.")

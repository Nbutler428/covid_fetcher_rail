import os
import requests
import psycopg2
from datetime import datetime


DATABASE_URL = os.environ['DATABASE_URL']
START_DATE = os.environ.get('START_DATE', datetime.now().strftime('%Y-%m-%d'))
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')

today = datetime.now().strftime('%Y-%m-%dT00:00:00.000')
url = f"https://data.cityofnewyork.us/resource/rc75-m7u3.json?$where=date_of_interest='{today}'"
print(f"Fetching today's data from: {url}")


response = requests.get(url)
data = response.json()


if not data:
    print("No data returned from the API. Please check your START_DATE or the API URL.")
    exit(1)


columns = list(data[0].keys())
columns_sql = ', '.join(columns)
placeholders = ', '.join(['%s'] * len(columns))


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

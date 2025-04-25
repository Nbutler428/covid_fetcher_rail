import os
import requests
import psycopg2
from datetime import datetime


DATABASE_URL = os.environ['DATABASE_URL']
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')


url = (
    "https://data.cityofnewyork.us/resource/rc75-m7u3.json?"
    "$where=date_of_interest>'2025-03-31T00:00:00.000'&$limit=1000")
print(f" Fetching data from: {url}")


response = requests.get(url)
data = response.json()

if not data:
    print("ruh ro somethin went wrongy")
    exit(0)


columns = list(data[0].keys())
columns_sql = ', '.join(columns)
placeholders = ', '.join(['%s'] * len(columns))


try:
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
    print("wow it actually worked")

except Exception as e:
    print("Dang nabbit not again")
    print(e)

finally:
    if 'cur' in locals(): cur.close()
    if 'conn' in locals(): conn.close()

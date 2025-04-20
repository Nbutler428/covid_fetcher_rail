import os
import requests
import psycopg2


DATABASE_URL = os.environ['DATABASE_URL']
TABLE_NAME = os.environ.get('TABLE_NAME', 'covid_data')


target_date = '2025-03-31T00:00:00.000'
url = f"https://data.cityofnewyork.us/resource/rc75-m7u3.json?$where=date_of_interest='{target_date}'"
print(f"Fetching data from: {url}")


response = requests.get(url)
data = response.json()


if not data:
    print(f"No data returned for {target_date}.")
    exit(0)


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


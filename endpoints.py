from fastapi import FastAPI
import pyodbc
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()
app = FastAPI()

def get_conn():
    return pyodbc.connect("DRIVER={ODBC DRIVER 18 for SQL Server};"
                      "SERVER=localhost,1434"
                      "DATABASE=gold"
                      "UID=sa;"
                      f"PWD={os.getenv("DB_PASSWORD")}"
                      "TrustServerCertificate=yes")


@app.get("/infer_schedule/{route}/{is_weekend}")
def infer_schedule(route: int, is_weekend: bool):
    """
       Get inferred timetable for a route.

       Returns average arrival times at each stop, derived from historical
       vehicle tracking data. Times are grouped by stop name.

       - **route_id**: Route number (e.g., 4, 10, 22)
       - **is_weekend**: True for weekend schedule, False for weekdays
    """

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT stop_name, avg_arrival_time, trip_sequence
        FROM gold.inferred_schedule
        WHERE route_id = ? AND is_weekend = ?
        ORDER BY stop_name, trip_sequence   
        """, (route, int(is_weekend))
    )

    mapped_schedule = defaultdict(list)
    for stop_name, arrival_time, _ in cursor.fetchall():
        mapped_schedule[stop_name].append(arrival_time)

    cursor.close()
    conn.close()
    return dict(mapped_schedule)


@app.get("/arrivals/{route_id}")
def get_arrivals(route_id: int, date_from: str = None, date_to: str = None):
    """Get daily arrival statistics per stop.

    Shows how many buses actually arrived at each stop, how many unique
    vehicles served it, and the average time between arrivals (headway).

    - **route_id**: Route number
    - **date_from**: Start date filter (YYYY-MM-DD)
    - **date_to**: End date filter (YYYY-MM-DD)"""
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT stop_id, date_key, actual_arrivals, vehicles_serving,
               first_arrival, last_arrival, avg_headway_minutes
        FROM gold.agg_arrivals_daily
        WHERE route_id = ?
    """
    params = [route_id]

    if date_from:
        query += " AND date_key >= ?"
        params.append(date_from)
    if date_to:
        query += " AND date_key <= ?"
        params.append(date_to)

    query += " ORDER BY date_key, stop_id"

    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return rows


@app.get("/speed/{route_id}")
def get_speed(route_id: int, date_from: str = None, date_to: str = None):
    """Get hourly speed statistics for a route.

    Shows average, min, and max vehicle speeds per hour. Useful for
    identifying congestion patterns throughout the day.

    - **route_id**: Route number
    - **date_from**: Start date filter (YYYY-MM-DD)
    - **date_to**: End date filter (YYYY-MM-DD)"""
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT hour, date_key, avg_speed, min_speed, max_speed, reading_count
        FROM gold.agg_speed_hourly
        WHERE route_id = ?
    """
    params = [route_id]

    if date_from:
        query += " AND date_key >= ?"
        params.append(date_from)
    if date_to:
        query += " AND date_key <= ?"
        params.append(date_to)

    query += " ORDER BY date_key, hour"

    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return rows
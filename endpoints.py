from fastapi import FastAPI
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

def get_conn():
    return pyodbc.connect("DRIVER={ODBC DRIVER 18 for SQL Server};"
                      "SERVER=localhost,1434"
                      "DATABASE=gold"
                      "UID=sa;"
                      f"PWD={os.getenv("DB_PASSWORD")}"
                      "TrustServerCertificate=yes")


@app.get("/infer_schedule/{route}/{day_of_week}")
def infer_schedule(route: int, day_of_week: int):
    if 0 < day_of_week < 8:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT ")



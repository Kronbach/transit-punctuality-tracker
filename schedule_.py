import schedule
import time
from datetime import datetime
from spark_script import *
from threading import Thread


def is_operating_hours():
    hour = datetime.now().hour
    # 6:00 to 23:59 OR 0:00 to 1:00
    return 6 <= hour or hour < 1

def poll_vehicles():
    while True:
        if is_operating_hours():
            write_bronze_df({"vehicles": (source.get_vehicles(), "append")})
            print(f"Polled at {datetime.now()}")
            time.sleep(25)



def run_bronze_to_silver():
    """
    Runs daily at 2am - reads fresh from Bronze
    """
    bronze_vehicles = spark.read.format("delta").load(f"{BRONZE}/vehicles")
    bronze_stops = spark.read.format("delta").load(f"{BRONZE}/stops")
    bronze_stop_times = spark.read.format("delta").load(f"{BRONZE}/stop_times")

    create_fact_vehicle_speed(spark=spark, bronze_vehicles=bronze_vehicles)
    create_fact_stop_arrival(spark=spark, bronze_vehicles=bronze_vehicles, bronze_stops=bronze_stops, bronze_stop_times=bronze_stop_times)
    print(f"Silver transforms complete at {datetime.now()}")

def start_poller_thread():
    Thread(target=poll_vehicles, daemon=True).start()

schedule.every().day.at("02:02", "Europe/Bucharest").do(run_bronze_to_silver)
schedule.every().day.at("06:00", "Europe/Bucharest").do(start_poller_thread)

if is_operating_hours():
    start_poller_thread()

while True:
    schedule.run_pending()
    time.sleep(1)
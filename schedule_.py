import schedule
import time
from datetime import datetime, date, timedelta
from spark_script import *


def _is_operating_hours():
    hour = datetime.now().hour
    # 6:00 to 23:59 OR 0:00 to 1:00
    return 6 <= hour or hour < 1


def poll_vehicles():
    while _is_operating_hours():
        write_bronze_df({"vehicles": (source.get_vehicles(), "append")})
        print(f"Polled at {datetime.now()}")
        time.sleep(25)


def poll_route_planning():
    write_bronze_df(
        {
            "stops": (source.get_stops(), "overwrite"),
            "stop_times": (source.get_stop_times(), "overwrite"),
            "trips": (source.get_trips(), "overwrite"),
            "routes": (source.get_routes(), "overwrite"),
        }
    )
    print(f"Polled at {datetime.now()}")


def run_bronze_to_silver(rebuild=False):
    """
    Runs daily at 2am - reads fresh from Bronze
    """
    if rebuild:
        import shutil

        shutil.rmtree(SILVER, ignore_errors=True)
        os.makedirs(SILVER, exist_ok=True)
        bronze_vehicles = spark.read.format("delta").load(f"{BRONZE}/vehicles")

    else:
        yesterday = date.today() - timedelta(days=1)
        bronze_vehicles = (
            spark.read.format("delta")
            .load(f"{BRONZE}/vehicles")
            .filter(f"_ingest_date = '{yesterday}'")
        )

    bronze_stops = spark.read.format("delta").load(f"{BRONZE}/stops")
    bronze_stop_times = spark.read.format("delta").load(f"{BRONZE}/stop_times")

    create_fact_vehicle_speed(spark=spark, bronze_vehicles=bronze_vehicles)
    create_fact_stop_arrival(
        spark=spark,
        bronze_vehicles=bronze_vehicles,
        bronze_stops=bronze_stops,
        bronze_stop_times=bronze_stop_times,
    )
    print(f"Silver transforms complete at {datetime.now()}")


def run_silver_to_gold():
    bronze_stop_times = spark.read.format("delta").load(f"{BRONZE}/stop_times")
    bronze_trips = spark.read.format("delta").load(f"{BRONZE}/trips")
    fact_vehicle_speed = spark.read.format("delta").load(f"{SILVER}/fact_vehicle_speed")
    fact_arrivals = spark.read.format("delta").load(f"{SILVER}/fact_stop_arrival")
    create_gold(
        fact_vehicle_speed=fact_vehicle_speed,
        fact_arrivals=fact_arrivals,
        bronze_trips=bronze_trips,
        bronze_stop_times=bronze_stop_times,
    )


schedule.every().day.at("02:02", "Europe/Bucharest").do(run_bronze_to_silver)
schedule.every(4).weeks.at("01:00", "Europe/Bucharest").do(poll_route_planning)
schedule.every().day.at("06:00", "Europe/Bucharest").do(poll_vehicles)
schedule.every().week.at("02:30", "Europe/Bucharest").do(run_silver_to_gold)

while True:
    schedule.run_pending()
    time.sleep(1)

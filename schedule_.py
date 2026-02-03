import schedule
import time
from datetime import datetime
from spark_script import *
from helpers import _utils

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
    _utils.prepare_views(spark=spark, bronze_path=BRONZE, silver_rebuild=rebuild)

    create_silver_dimensions(spark=spark, bronze_path=BRONZE, silver_path=SILVER)
    create_fact_vehicle_speed(spark=spark)
    create_fact_stop_arrival(spark=spark)
    print(f"Silver transforms complete at {datetime.now()}")


def run_silver_to_gold():
    _utils.prepare_views(spark=spark, silver_path=SILVER)

    fact_vehicle_speed = spark.read.format("delta").load(f"{SILVER}/fact_vehicle_speed")
    fact_arrivals = spark.read.format("delta").load(f"{SILVER}/fact_stop_arrival")

    create_gold(
        fact_vehicle_speed=fact_vehicle_speed,
        fact_arrivals=fact_arrivals
    )

#
# schedule.every().day.at("02:02", "Europe/Bucharest").do(run_bronze_to_silver)
# schedule.every(30).days.at("01:00", "Europe/Bucharest").do(poll_route_planning)
# schedule.every().day.at("06:00", "Europe/Bucharest").do(poll_vehicles)
# schedule.every(7).days.at("02:30", "Europe/Bucharest").do(run_silver_to_gold)
run_silver_to_gold()

# while True:
#     schedule.run_pending()
#     time.sleep(1)

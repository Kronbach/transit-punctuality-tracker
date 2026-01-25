import schedule
import time
from datetime import datetime
from spark_script import *


def is_operating_hours():
    hour = datetime.now().hour
    # 6:00 to 23:59 OR 0:00 to 1:00
    return 6 <= hour or hour < 1

while True:
    if is_operating_hours():
        mapped_dataframes = write_bronze_df(endpoints)
    time.sleep(19)


schedule.every().day.at("02:02", "Europe/Amsterdam").do(create_fact_vehicle_speed(
    spark=spark_script.spark,
    bronze_vehicles=mapped_dataframes["vehicles"]
))

schedule.every().day.at("02:00", "Europe/Amsterdam").do(create_fact_stop_arrival(
    spark=spark_script.spark,
    bronze_stops=mapped_dataframes["stops"],
    bronze_stop_times=mapped_dataframes["stop_times"],
    bronze_vehicles=mapped_dataframes["vehicles"]
))

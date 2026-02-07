import schedule
import time
from datetime import datetime
from spark_script import *
from helpers import _utils
from collections import deque
import threading

class Buffer:
    """
       Thread-safe buffer for decoupling vehicle data fetching from Delta Lake writes.

       Solves the problem of slow Spark writes (~90s) blocking frequent API polling (25s).
       One thread fetches data into the buffer, another flushes to Bronze when full.

       Usage:
           buffer = Buffer()
           poll_vehicles(buffer)

       Attributes:
           container: deque holding batches of vehicle data
           buffer_lock: threading lock for safe concurrent access
           running: flag to coordinate thread lifecycle

       Flush triggers:
           - 24+ batches accumulated (~10 min of data)
           - End of operating hours (final flush of remaining data)
       """
    def __init__(self):
        self.container = deque(maxlen=100)
        self.buffer_lock = threading.Lock()
        self.running = False

    def load_buffer(self):
        while self.running and _utils.is_operating_hours():
            data = source.get_vehicles()
            with self.buffer_lock:
                self.container.append(data)
            print(f"Buffered at {datetime.now()} ({len(self.container)} batches)", flush=True)
            time.sleep(25)

#TODO: Is it really okay to duplicate the code here ?
    def flush_buffer(self):
        while self.running or self.container:
            if len(self.container) >= 24:

                with self.buffer_lock:
                    combined = [v for batch in self.container for v in batch]
                    self.container.clear()
                write_bronze_df({"vehicles": (combined, "append")})
                print(f"Flushed {len(combined)} vehicles to Bronze at {datetime.now()}", flush=True)

            elif self.container and not self.running:

                with self.buffer_lock:
                    combined = [v for batch in self.container for v in batch]
                    self.container.clear()
                write_bronze_df({"vehicles": (combined, "append")})
                print(f"Final flush {len(combined)} vehicles to Bronze at {datetime.now()}", flush=True)

            else:
                time.sleep(5)


def poll_vehicles(buffer: Buffer):
    buffer.running = True

    polling_thread = threading.Thread(target=buffer.load_buffer)
    write_thread = threading.Thread(target=buffer.flush_buffer)

    polling_thread.start()
    write_thread.start()

    polling_thread.join()
    buffer.running = False
    write_thread.join()


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


# schedule.every().day.at("02:02", "Europe/Bucharest").do(run_bronze_to_silver)
# schedule.every(30).days.at("01:00", "Europe/Bucharest").do(poll_route_planning)
# schedule.every().day.at("06:00", "Europe/Bucharest").do(poll_vehicles)
# schedule.every(7).days.at("02:30", "Europe/Bucharest").do(run_silver_to_gold)

buffer = Buffer()
poll_vehicles(buffer)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

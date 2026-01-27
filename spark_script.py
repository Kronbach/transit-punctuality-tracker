from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, col, current_timestamp, hour

from sources import TranzyAPI

from pyspark.sql import SparkSession, Row
import os
import json
from dotenv import load_dotenv

load_dotenv()
spark = (SparkSession.builder.appName("public_transport_etl")
         .getOrCreate())
source = TranzyAPI(os.getenv("TRANZY_KEY"))

BRONZE = "/mnt/z/public_transport/bronze"
SILVER = "/mnt/z/public_transport/silver"

endpoints = {
            "vehicles": (source.get_vehicles(), "append"),
            "stops": (source.get_stops(), "overwrite"),
            "stop_times": (source.get_stop_times(), "overwrite"),
            "trips": (source.get_trips(), "overwrite"),
            "routes": (source.get_routes(), "overwrite")
         }

def create_bronze_df(spark, data: dict):
    rows = [(json.dumps(item),) for item in data]
    return spark.createDataFrame(rows, ["json"]) \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_ingest_date", to_date(col("_ingest_ts")))

def write_bronze_df(endpoints: dict[str, tuple[dict, str]]) -> dict[str, DataFrame]:
    mapped_dataframes = {}
    for name, (data, mode) in endpoints.items():
        bronze_df = create_bronze_df(spark, data)
        bronze_df.write.format("delta").mode(mode).save(f"{BRONZE}/{name}")
        mapped_dataframes[name] = bronze_df
    return mapped_dataframes

#In production fact tables need to read from Delta - not from memory (Dataframe). Use this workflow just for tests.
def create_fact_vehicle_speed(spark, bronze_vehicles) -> DataFrame:
    """
    Grain: one row per vehicle per poll
    """
    parsed = spark.read.json(
        bronze_vehicles.select("json").rdd.map(lambda r: r[0])
    )
    parsed.createOrReplaceTempView("vehicles")

    fact = spark.sql("""
        SELECT
            id AS vehicle_id,
            route_id,
            trip_id,
            latitude,
            longitude,
            speed,
            timestamp,
            HOUR(timestamp) AS hour,
            TO_DATE(timestamp) AS date_key
        FROM vehicles
        WHERE trip_id IS NOT NULL
        """)

    fact.write.format("delta").mode("append").save(f"{SILVER}/fact_vehicle_position")
    return fact
#In production fact tables need to read from Delta - not from memory (Dataframe). Use this workflow just for tests.
def create_fact_stop_arrival(spark, bronze_vehicles, bronze_stops, bronze_stop_times) -> DataFrame:
    """
    Grain: one row per vehicle arrival at a stop
    """
    vehicles = spark.read.json(
        bronze_vehicles.select("json").rdd.map(lambda r: r[0])
    )
    stops = spark.read.json(
        bronze_stops.select("json").rdd.map(lambda r: r[0])
    )
    stop_times = spark.read.json(
        bronze_stop_times.select("json").rdd.map(lambda r: r[0])
    )


    vehicles.createOrReplaceTempView("vehicles")
    stops.createOrReplaceTempView("stops")
    stop_times.createOrReplaceTempView("stop_times")

 # Join: vehicle -> stop_times -> stops
 # I need these joins to filter the stops designated for every vehicle based on its current trip.
    joined = spark.sql("""
        SELECT 
            v.id AS vehicle_id,
            v.route_id,
            v.trip_id,
            v.latitude AS v_lat,
            v.longitude AS v_lon,
            v.timestamp,
            st.stop_id,
            st.stop_sequence,
            s.stop_lat AS s_lat,
            s.stop_lon AS s_lon,
            s.stop_name
        FROM vehicles v
        JOIN stop_times st ON v.trip_id = st.trip_id
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE v.trip_id IS NOT NULL
    """)

    joined.createOrReplaceTempView("vehicle_stops")

    # The big equation thingy is just to transform the coordinate difference
    # into actual measurable meters.
    # Store the distance difference in the CTE first and then filter on it, ez.
    arrivals = spark.sql("""
        WITH vehicle_distances AS (
        SELECT 
            vehicle_id,
            route_id,
            trip_id,
            stop_id,
            stop_name,
            stop_sequence,
            timestamp AS arrival_timestamp,
            HOUR(timestamp) AS hour,
            TO_DATE(timestamp) AS date_key,
            6371000 * 2 * ATAN2(
                SQRT(
                    POWER(SIN(RADIANS(s_lat - v_lat) / 2), 2) +
                    COS(RADIANS(v_lat)) * COS(RADIANS(s_lat)) *
                    POWER(SIN(RADIANS(s_lon - v_lon) / 2), 2)
                ),
                SQRT(1 - (
                    POWER(SIN(RADIANS(s_lat - v_lat) / 2), 2) +
                    COS(RADIANS(v_lat)) * COS(RADIANS(s_lat)) *
                    POWER(SIN(RADIANS(s_lon - v_lon) / 2), 2)
                ))
            ) AS distance_m
        FROM vehicle_stops
        )
        SELECT *
        FROM vehicle_distances
        WHERE distance_m < 50
    """)
    arrivals.write.format("delta").mode("append").save(f"{SILVER}/fact_stop_arrival")
    return arrivals



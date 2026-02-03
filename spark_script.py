import os
import json
from dotenv import load_dotenv
from helpers import _utils
load_dotenv()

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, col, current_timestamp
from sources import TranzyAPI
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("public_transport_etl")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
source = TranzyAPI(os.getenv("TRANZY_KEY"))

BRONZE = "/mnt/z/public_transport/bronze"
SILVER = "/mnt/z/public_transport/silver"

endpoints = {
    "vehicles": (source.get_vehicles(), "append"),
    "stops": (source.get_stops(), "overwrite"),
    "stop_times": (source.get_stop_times(), "overwrite"),
    "trips": (source.get_trips(), "overwrite"),
    "routes": (source.get_routes(), "overwrite"),
}


def _create_bronze_df(spark, data: dict):
    rows = [(json.dumps(item),) for item in data]
    return (
        spark.createDataFrame(rows, ["json"])
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_ingest_date", to_date(col("_ingest_ts")))
    )


def write_bronze_df(endpoints: dict[str, tuple[dict, str]]) -> dict[str, DataFrame]:
    mapped_dataframes = {}
    for name, (data, mode) in endpoints.items():
        bronze_df = _create_bronze_df(spark, data)
        bronze_df.write.format("delta").mode(mode).partitionBy("_ingest_date").save(
            f"{BRONZE}/{name}"
        )
        mapped_dataframes[name] = bronze_df
    return mapped_dataframes


def create_silver_dimensions(spark, bronze_path, silver_path):
    """Parse Bronze JSON and persist dimensions to Silver."""

    for table in ["stops", "trips", "stop_times"]:
        df = _utils.parse_bronze_json(spark, bronze_path, table)
        df.write.format("delta").mode("overwrite").save(f"{silver_path}/dim_{table}")
        print(f"dim_{table}: {df.count()} rows")


def create_fact_vehicle_speed(spark) -> DataFrame:
    """
    Grain: one row per vehicle per poll
    """

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


    fact.write.format("delta").mode("append").save(f"{SILVER}/fact_vehicle_speed")
    return fact


def create_fact_stop_arrival(spark) -> DataFrame:
    """
    Grain: one row per vehicle arrival at a stop
    """
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
            CAST(timestamp AS TIMESTAMP) AS arrival_timestamp,
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
        ),
        
        
    arrivals_raw AS(
        SELECT *
        FROM vehicle_distances
        WHERE distance_m < 20),
    
    
    deduplicated AS (
        SELECT *,
        LAG(arrival_timestamp) OVER (
            PARTITION BY vehicle_id, stop_id 
            ORDER BY arrival_timestamp
        ) as prev_arrival
        FROM arrivals_raw
        )
        
        
        SELECT * FROM deduplicated
        WHERE prev_arrival IS NULL 
        OR arrival_timestamp > prev_arrival + INTERVAL 25 MINUTES 
    """)
    arrivals.write.format("delta").mode("append").save(f"{SILVER}/fact_stop_arrival")


    return arrivals


def create_gold(fact_vehicle_speed, fact_arrivals):

    fact_vehicle_speed.createOrReplaceTempView("fact_vehicle_speed")
    fact_arrivals.createOrReplaceTempView("fact_arrivals")

    agg_speed_hourly = spark.sql(
        """
    SELECT
        route_id,
        hour,
        date_key,
        DAYOFWEEK (date_key) as day_of_week,
        MONTH(date_key) as month,
        CAST(AVG(speed) AS DECIMAL(4,1)) as avg_speed,
        CASE
            WHEN DAYOFWEEK(date_key) IN (1, 7) THEN 1
            ELSE 0
        END AS is_weekend,
        COUNT(vehicle_id) as sample_count
        
    FROM fact_vehicle_speed
    GROUP BY route_id, date_key, hour
    """
    )

    agg_arrivals_daily = spark.sql("""
        SELECT 
            stop_id,
            route_id,
            TO_DATE(arrival_timestamp) as arrival_date,
            COUNT(*) as actual_arrivals,
            COUNT(DISTINCT vehicle_id) as vehicles_serving,
            MIN(arrival_timestamp) as first_arrival,
            MAX(arrival_timestamp) as last_arrival
        FROM fact_arrivals
        GROUP BY stop_id, route_id, arrival_date
    """)

    inferred_schedule = spark.sql("""
     WITH ordered_arrivals AS (
        SELECT 
            fa.route_id,
            fa.stop_id,
            s.stop_name,
            fa.vehicle_id,
            fa.date_key,
            fa.arrival_timestamp,
            DATE_FORMAT(fa.arrival_timestamp, 'HH:mm') as arrival_time,
            ROW_NUMBER() OVER (
                PARTITION BY fa.route_id, fa.stop_id, fa.date_key 
                ORDER BY fa.arrival_timestamp
            ) as trip_sequence
        FROM fact_arrivals fa
        JOIN stops s ON fa.stop_id = s.stop_id
    )
    SELECT 
        route_id,
        stop_id,
        stop_name,
        vehicle_id,
        date_key,
        trip_sequence,
        arrival_time,
        arrival_timestamp
    FROM ordered_arrivals
    """)

    agg_speed_hourly.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.agg_speed_hourly",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

    agg_arrivals_daily.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.agg_arrivals_daily",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

    inferred_schedule.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.inferred_schedule",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

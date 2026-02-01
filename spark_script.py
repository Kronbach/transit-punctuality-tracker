import os
import json
from dotenv import load_dotenv

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


def create_fact_vehicle_speed(spark, bronze_vehicles) -> DataFrame:
    """
    Grain: one row per vehicle per poll
    """
    parsed = spark.read.json(bronze_vehicles.select("json").rdd.map(lambda r: r[0]))
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

    fact.write.format("delta").mode("append").save(f"{SILVER}/fact_vehicle_speed")
    return fact


def create_fact_stop_arrival(
    spark, bronze_vehicles, bronze_stops, bronze_stop_times
) -> DataFrame:
    """
    Grain: one row per vehicle arrival at a stop
    """

    vehicles = spark.read.json(bronze_vehicles.select("json").rdd.map(lambda r: r[0]))
    stops = spark.read.json(bronze_stops.select("json").rdd.map(lambda r: r[0]))
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


def create_gold(fact_vehicle_speed, fact_arrivals, bronze_trips, bronze_stop_times):
    bronze_stop_times_df = spark.read.json(
        bronze_stop_times.select("json").rdd.map(lambda r: r[0])
    )

    bronze_trips_df = spark.read.json(
        bronze_trips.select("json").rdd.map(lambda r: r[0])
    )
    bronze_stop_times_df.createOrReplaceTempView("stop_times")
    bronze_trips_df.createOrReplaceTempView("trips")
    fact_vehicle_speed.createOrReplaceTempView("fact_vehicle_speed")
    fact_arrivals.createOrReplaceTempView("fact_arrivals")

    agg_speed_hourly = spark.sql(
        """
    SELECT
        route_id,
        hour,
        date_key,
        DAYOFWEEK (date_key) as day_of_week,
        MONTH(date_key) as day_of_month,
        AVG(speed) as avg_speed,
        CASE
            WHEN DAYOFWEEK(date_key) IN (1, 7) THEN 1
            ELSE 0
        END AS is_weekend,
        COUNT(vehicle_id) as sample_count
        
    FROM fact_vehicle_speed
    GROUP BY route_id, date_key, hour
    """
    )

    agg_arrivals_adherence = spark.sql(
        """
     WITH expected AS (
        SELECT 
            t.route_id,
            st.stop_id,
            COUNT(DISTINCT st.trip_id) as expected_daily_arrivals
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        GROUP BY t.route_id, st.stop_id
    ),
    actual AS (
        SELECT 
            route_id,
            stop_id,
            date_key,
            COUNT(*) as actual_arrivals
        FROM fact_arrivals
        GROUP BY route_id, stop_id, date_key
    )
    SELECT 
        a.route_id,
        a.stop_id,
        a.date_key,
        e.expected_daily_arrivals as expected,
        a.actual_arrivals as actual,
        a.actual_arrivals - e.expected_daily_arrivals as delta
    FROM actual a
    JOIN expected e 
        ON a.route_id = e.route_id 
        AND a.stop_id = e.stop_id
"""
    )

    inferred_schedule = spark.sql("""
    WITH ordered_arrivals AS (
    SELECT 
        route_id,
        stop_id,
        date_key,
        DAYOFWEEK(date_key) as day_of_week,
        arrival_timestamp,
        hour * 60 + MINUTE(arrival_timestamp) as minutes_of_day,
        ROW_NUMBER() OVER (
            PARTITION BY route_id, stop_id, date_key 
            ORDER BY arrival_timestamp
        ) as trip_sequence
    FROM fact_arrivals
    )
    SELECT
        route_id,
        stop_id,
        day_of_week,
        trip_sequence,
        AVG(minutes_of_day) as avg_arrival_minutes,
        COUNT(*) as sample_count
    FROM ordered_arrivals
    GROUP BY route_id, stop_id, day_of_week, trip_sequence
    """)

    agg_speed_hourly.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.agg_speed_hourly",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

    agg_arrivals_adherence.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.agg_arrivals_adherence",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

    inferred_schedule.write.jdbc(
        url=os.getenv("DB_URL"),
        table="gold.inferred_schedule",
        mode="overwrite",
        properties={"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASSWORD")},
    )

from pyspark.sql.functions import current_date, date_sub, col

def parse_bronze_json(spark, bronze_path, table, yesterday_filter=True):
    """Parse JSON from Bronze Delta table."""
    if yesterday_filter and table == "vehicles":
        bronze_df = spark.read.format("delta").load(f"{bronze_path}/{table}").filter(col("_ingest_date") == date_sub(current_date(), 1))
    else:
        bronze_df = spark.read.format("delta").load(f"{bronze_path}/{table}")
    return spark.read.json(bronze_df.select("json").rdd.map(lambda r: r[0]))


def prepare_views(spark, bronze_path=None, silver_path=None, silver_rebuild=False):
    """
    Prepare temp views for transforms.
    - bronze_path: parse from Bronze JSON (for Bronze→Silver)
    - silver_path: load from Silver Delta (for Silver→Gold)
    """

    if bronze_path:
        stops = parse_bronze_json(spark, bronze_path, "stops")
        trips = parse_bronze_json(spark, bronze_path, "trips")
        stop_times = parse_bronze_json(spark, bronze_path, "stop_times")
        vehicles = parse_bronze_json(spark, bronze_path, "vehicles", yesterday_filter=not silver_rebuild)



        vehicles.createOrReplaceTempView("vehicles_raw")
        stop_times.createOrReplaceTempView("stop_times_raw")

        # Deduplication
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW vehicles AS
            SELECT DISTINCT id, route_id, trip_id, latitude, longitude, speed, timestamp
            FROM vehicles_raw
            WHERE trip_id IS NOT NULL
        """)

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW stop_times AS
            SELECT trip_id, stop_id, MIN(stop_sequence) as stop_sequence
            FROM stop_times_raw
            GROUP BY trip_id, stop_id
        """)

    elif silver_path:
        # Load parsed Delta tables
        stops = spark.read.format("delta").load(f"{silver_path}/dim_stops")
        trips = spark.read.format("delta").load(f"{silver_path}/dim_trips")
        stop_times = spark.read.format("delta").load(f"{silver_path}/dim_stop_times")
        stop_times.cache().createOrReplaceTempView("stop_times")
        # Common: register dimensions (cache for Gold)

    stops.cache().createOrReplaceTempView("stops")
    trips.cache().createOrReplaceTempView("trips")

    print(f"Views ready: {stops.count()} stops, {trips.count()} trips")

    return {"stops": stops, "trips": trips}
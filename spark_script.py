from pyspark.sql.functions import to_date, col, current_timestamp

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

endpoints = {
            "vehicles": (source.get_vehicles(), "append"),
            "stops": (source.get_stops(), "overwrite"),
            "stop_times": (source.get_stop_times(), "overwrite"),
            "trips": (source.get_trips(), "overwrite"),
            "routes": (source.get_routes(), "overwrite")
         }

def create_bronze_df(spark, data: dict):
    rows = [Row(json=item) for item in data]
    return spark.createDataFrame(rows) \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_ingest_date", to_date(col("_ingest_ts")))

def write_bronze_df(endpoints: dict[str, tuple[dict, str]]) -> None:
    for name, (data, mode) in endpoints.items():
        bronze_df = create_bronze_df(spark, data)
        bronze_df.write.format("delta").mode(mode).save(f"{BRONZE}/{name}")

spark.read.format("delta").load("/mnt/z/public_transport/bronze/vehicles").show(5, truncate=True)


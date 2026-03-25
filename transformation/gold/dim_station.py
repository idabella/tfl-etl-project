from pyspark.sql import SparkSession, functions as F

SILVER_STOPPOINTS = "s3a://tfl-silver-bucket/stoppoints/"
SILVER_ARRIVALS   = "s3a://tfl-silver-bucket/arrivals/"
GOLD_PATH         = "s3a://tfl-gold-bucket/dimensions/dim_station/"

def main():
    spark = SparkSession.builder.appName("gold_dim_station").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_stops    = spark.read.parquet(SILVER_STOPPOINTS)
    df_arrivals = spark.read.parquet(SILVER_ARRIVALS)

    df_arrival_stations = (
        df_arrivals
        .select(F.col("naptanId").alias("naptan_id"), F.col("stationName").alias("station_name"))
        .dropDuplicates(["naptan_id"])
    )

    dim_station = (
        df_stops
        .select(
            F.col("naptanId").alias("naptan_id"),
            F.col("commonName").alias("common_name"),
            F.col("stopType").alias("stop_type"),
            F.col("stationNaptan").alias("station_naptan"),
            F.col("zone"), F.col("wifi"), F.col("modes"),
            F.col("lat"), F.col("lon"), F.col("status")
        )
        .join(df_arrival_stations, on="naptan_id", how="left")
        .withColumn("display_name",    F.coalesce(F.col("station_name"), F.col("common_name")))
        .withColumn("zone_primary",    F.split(F.col("zone"), "/")[0])
        .withColumn("zone_secondary",  F.split(F.col("zone"), "/")[1])
        .withColumn("has_wifi",        F.lower(F.col("wifi")) == "yes")
        .withColumn("station_key",     F.monotonically_increasing_id().cast("integer"))
        .withColumn("_created_at",     F.current_timestamp())
        .dropDuplicates(["naptan_id"])
    )

    dim_station.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_station written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
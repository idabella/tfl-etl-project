from pyspark.sql import SparkSession, functions as F

SILVER_ARRIVALS = "s3a://tfl-silver-bucket/arrivals/"
DIM_DATE        = "s3a://tfl-gold-bucket/dimensions/dim_date/"
DIM_TIME        = "s3a://tfl-gold-bucket/dimensions/dim_time/"
DIM_LINE        = "s3a://tfl-gold-bucket/dimensions/dim_line/"
DIM_STATION     = "s3a://tfl-gold-bucket/dimensions/dim_station/"
GOLD_PATH       = "s3a://tfl-gold-bucket/facts/fact_arrivals/"

def main():
    spark = SparkSession.builder.appName("gold_fact_arrivals").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df          = spark.read.parquet(SILVER_ARRIVALS)
    dim_date    = spark.read.parquet(DIM_DATE).select("date_key", "full_date")
    dim_time    = spark.read.parquet(DIM_TIME).select("time_key", "time_str", "period_of_day", "is_peak_hour")
    dim_line    = spark.read.parquet(DIM_LINE).select("line_key", "line_id")
    dim_station = spark.read.parquet(DIM_STATION).select("station_key", "naptan_id")
    df = (
        df
        .withColumn("date_key", F.date_format("timestamp", "yyyyMMdd").cast("integer"))
        .withColumn("time_key", (F.hour("timestamp") * 60 + F.minute("timestamp")).cast("integer"))
    )
    fact = (
        df
        .join(dim_date,    on="date_key",                              how="left")
        .join(dim_time,    on="time_key",                              how="left")
        .join(dim_line,    df["lineId"] == dim_line["line_id"],        how="left")
        .join(dim_station, df["naptanId"] == dim_station["naptan_id"], how="left")
        .select(
            F.col("date_key"), F.col("time_key"), F.col("line_key"), F.col("station_key"),
            F.col("id").alias("arrival_id"),
            F.col("vehicleId").alias("vehicle_id"),
            F.col("naptanId").alias("naptan_id"),
            F.col("lineId").alias("line_id"),
            F.col("platformName").alias("platform_name"),
            F.col("direction"),
            F.col("destinationNaptanId").alias("destination_naptan_id"),
            F.col("destinationName").alias("destination_name"),
            F.col("timeToStation").alias("time_to_station_seconds"),
            (F.col("timeToStation") / 60).alias("time_to_station_minutes"),
            F.col("currentLocation").alias("current_location"),
            F.col("towards"), F.col("modeName").alias("mode_name"),
            F.col("timestamp").alias("prediction_timestamp"),
            F.col("expectedArrival").alias("expected_arrival"),
            F.col("timeToLive").alias("time_to_live"),
            F.col("period_of_day"), F.col("is_peak_hour"),
            F.col("_ingested_at"), F.col("_silver_processed_at"),
            F.current_timestamp().alias("_gold_processed_at"),
            F.col("_year"), F.col("_month"), F.col("_day"),
        )
    )
    fact.write.mode("overwrite").partitionBy("_year", "_month", "_day").parquet(GOLD_PATH)
    print(f"fact_arrivals written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
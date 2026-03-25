from pyspark.sql import SparkSession, functions as F

SILVER_LINE_STATUS = "s3a://tfl-silver-bucket/line_status/"
DIM_DATE           = "s3a://tfl-gold-bucket/dimensions/dim_date/"
DIM_TIME           = "s3a://tfl-gold-bucket/dimensions/dim_time/"
DIM_LINE           = "s3a://tfl-gold-bucket/dimensions/dim_line/"
DIM_DISRUPTION     = "s3a://tfl-gold-bucket/dimensions/dim_disruption/"
GOLD_PATH          = "s3a://tfl-gold-bucket/facts/fact_line_status/"

def main():
    spark = SparkSession.builder.appName("gold_fact_line_status").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df             = spark.read.parquet(SILVER_LINE_STATUS)
    dim_date       = spark.read.parquet(DIM_DATE).select("date_key", "full_date")
    dim_time       = spark.read.parquet(DIM_TIME).select("time_key", "time_str", "period_of_day", "is_peak_hour")
    dim_line       = spark.read.parquet(DIM_LINE).select("line_key", "line_id")
    dim_disruption = spark.read.parquet(DIM_DISRUPTION).select("disruption_key", "severity_code")
    df = (
        df
        .withColumn("date_key", F.date_format("_ingested_at", "yyyyMMdd").cast("integer"))
        .withColumn("time_key", (F.hour("_ingested_at") * 60 + F.minute("_ingested_at")).cast("integer"))
    )
    fact = (
        df
        .join(dim_date,       on="date_key",                                           how="left")
        .join(dim_time,       on="time_key",                                           how="left")
        .join(dim_line,       df["id"] == dim_line["line_id"],                         how="left")
        .join(dim_disruption, df["statusSeverity"] == dim_disruption["severity_code"], how="left")
        .select(
            F.col("date_key"), F.col("time_key"), F.col("line_key"), F.col("disruption_key"),
            F.col("id").alias("line_id"),
            F.col("name").alias("line_name"),
            F.col("modeName").alias("mode_name"),
            F.col("statusSeverity").alias("status_severity"),
            F.col("statusSeverityDescription").alias("status_description"),
            F.col("reason").alias("disruption_reason"),
            F.col("period_of_day"), F.col("is_peak_hour"),
            F.when(F.col("statusSeverity") == 10, True).otherwise(False).alias("is_good_service"),
            F.when(F.col("statusSeverity") < 10, True).otherwise(False).alias("is_disrupted"),
            F.col("_ingested_at"),
            F.col("modified").alias("last_modified"),
            F.current_timestamp().alias("_gold_processed_at"),
            F.col("_year"), F.col("_month"), F.col("_day"),
        )
    )
    fact.write.mode("overwrite").partitionBy("_year", "_month", "_day").parquet(GOLD_PATH)
    print(f"fact_line_status written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
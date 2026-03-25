from pyspark.sql import SparkSession, functions as F

GOLD_PATH = "s3a://tfl-gold-bucket/dimensions/dim_time/"

def main():
    spark = SparkSession.builder.appName("gold_dim_time").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dim_time = (
        spark.range(0, 1440)
        .withColumn("hour",   (F.col("id") / 60).cast("integer"))
        .withColumn("minute", (F.col("id") % 60).cast("integer"))
        .withColumn("time_key", F.col("id").cast("integer"))
        .withColumn("time_str", F.concat_ws(":",
            F.lpad(F.col("hour"), 2, "0"),
            F.lpad(F.col("minute"), 2, "0")))
        .withColumn("period_of_day",
            F.when(F.col("hour").between(5, 8),   "Early Morning")
             .when(F.col("hour").between(9, 11),  "Morning")
             .when(F.col("hour").between(12, 13), "Lunch")
             .when(F.col("hour").between(14, 16), "Afternoon")
             .when(F.col("hour").between(17, 19), "Evening Rush")
             .when(F.col("hour").between(20, 22), "Evening")
             .when(F.col("hour") == 23,           "Late Night")
             .otherwise("Night"))
        .withColumn("is_peak_hour", F.col("hour").between(7, 9) | F.col("hour").between(17, 19))
        .withColumn("_created_at", F.current_timestamp())
        .drop("id")
    )

    dim_time.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_time written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
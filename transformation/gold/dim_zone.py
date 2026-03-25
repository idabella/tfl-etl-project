from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://tfl-silver-bucket/stoppoints/"
GOLD_PATH   = "s3a://tfl-gold-bucket/dimensions/dim_zone/"

def main():
    spark = SparkSession.builder.appName("gold_dim_zone").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)

    dim_zone = (
        df.select(F.col("zone"))
        .filter(F.col("zone").isNotNull())
        .dropDuplicates(["zone"])
        .withColumn("zone_primary",      F.split(F.col("zone"), "/")[0])
        .withColumn("zone_secondary",    F.split(F.col("zone"), "/")[1])
        .withColumn("is_boundary_zone",  F.col("zone").contains("/"))
        .withColumn("zone_key",          F.monotonically_increasing_id().cast("integer"))
        .withColumn("zone_description",  F.concat(F.lit("Zone "), F.col("zone")))
        .withColumn("_created_at",       F.current_timestamp())
    )

    dim_zone.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_zone written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
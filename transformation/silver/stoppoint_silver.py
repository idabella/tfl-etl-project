from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, BooleanType

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.stoppoints/"
SILVER_PATH = "s3a://tfl-silver-bucket/stoppoints/"

def main():
    spark = SparkSession.builder.appName("stoppoint_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("inferSchema", "false").json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")

    df_silver = (
        df
        .select(
            F.col("id"),
            F.col("naptanId"),
            F.col("commonName"),
            F.col("placeType"),
            F.col("stopType"),
            F.col("stationNaptan"),
            F.col("indicator"),
            F.col("stopLetter"),
            F.col("status").cast(BooleanType()),
            F.col("lat").cast(DoubleType()),
            F.col("lon").cast(DoubleType()),
            F.expr("filter(additionalProperties, x -> x.key = 'Zone')[0].value").alias("zone"),
            F.expr("filter(additionalProperties, x -> x.key = 'WiFi')[0].value").alias("wifi"),
            F.array_join(F.col("modes"), ",").alias("modes"),
            F.col("_ingested_at"),
        )
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        .filter(F.col("id").isNotNull())
        .dropDuplicates(["id"])
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_year",  F.year("_ingested_at"))
        .withColumn("_month", F.month("_ingested_at"))
        .withColumn("_day",   F.dayofmonth("_ingested_at"))
    )

    print(f"Silver records: {df_silver.count()}")
    df_silver.write.mode("overwrite").partitionBy("_year", "_month", "_day").parquet(SILVER_PATH)
    print(f"Done: {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
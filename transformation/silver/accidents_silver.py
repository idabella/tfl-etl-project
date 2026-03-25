"""
Bronze → Silver: tfl.accidents
Reads raw JSON from S3 Bronze, explodes nested vehicles/casualties arrays,
cleans, types, deduplicates, writes Parquet to Silver.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from utils.spark_session import get_spark_session

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.accidents/"
SILVER_PATH  = "s3a://tfl-silver-bucket/accidents/"


def main():
    spark = get_spark_session("accidents_bronze_to_silver")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("inferSchema", "false").json(BRONZE_PATH)
    print(f"Bronze records read: {df.count()}")

    df_silver = (
        df
        # Cast coordinates
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))

        # Cast severity to integer
        .withColumn("severity", F.lower(F.trim(F.col("severity"))))

        # Cast timestamps
        .withColumn("date",         F.to_timestamp("date"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))

        # Clean strings
        .withColumn("borough",     F.trim("borough"))
        .withColumn("location",    F.trim("location"))

        # Flatten vehicles array → comma-separated string
        .withColumn(
            "vehicle_types",
            F.array_join(
                F.transform(F.col("vehicles"), lambda v: v.getField("type")),
                ", "
            )
        )

        # Flatten casualties: count by severity
        .withColumn(
            "casualties_total",
            F.size(F.col("casualties"))
        )
        .withColumn(
            "casualties_fatal",
            F.size(F.filter(F.col("casualties"), lambda c: c.getField("severity") == "Fatal"))
        )
        .withColumn(
            "casualties_serious",
            F.size(F.filter(F.col("casualties"), lambda c: c.getField("severity") == "Serious"))
        )
        .withColumn(
            "casualties_slight",
            F.size(F.filter(F.col("casualties"), lambda c: c.getField("severity") == "Slight"))
        )

        # Filter nulls
        .filter(F.col("id").isNotNull())

        # Deduplicate
        .dropDuplicates(["id"])

        # Audit columns
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_year",  F.year("date"))
        .withColumn("_month", F.month("date"))
        .withColumn("_day",   F.dayofmonth("date"))

        # Final column selection
        .select(
            "id",
            "lat", "lon",
            "location",
            "borough",
            "date",
            "severity",
            "vehicle_types",
            "casualties_total",
            "casualties_fatal",
            "casualties_serious",
            "casualties_slight",
            "_ingested_at",
            "_silver_processed_at",
            "_year", "_month", "_day",
        )
    )

    print(f"Silver records after cleaning: {df_silver.count()}")

    (
        df_silver.write
        .mode("append")
        .partitionBy("_year", "_month", "_day")
        .parquet(SILVER_PATH)
    )

    print(f"✅ accidents_silver: {df_silver.count()} records written to {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
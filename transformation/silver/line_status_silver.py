from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.line_status/"
SILVER_PATH = "s3a://tfl-silver-bucket/line_status/"

def main():
    spark = SparkSession.builder.appName("line_status_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("inferSchema", "false").json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")

    df_silver = (
        df
        .withColumn("lineStatus", F.explode_outer(F.col("lineStatuses")))
        .withColumn("statusSeverity",            F.col("lineStatus.statusSeverity").cast(IntegerType()))
        .withColumn("statusSeverityDescription", F.col("lineStatus.statusSeverityDescription"))
        .withColumn("reason",                    F.col("lineStatus.reason"))
        .withColumn("created",      F.to_timestamp("created"))
        .withColumn("modified",     F.to_timestamp("modified"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        .withColumn("name",     F.trim("name"))
        .withColumn("modeName", F.lower(F.trim("modeName")))
        .filter(F.col("id").isNotNull())
        .dropDuplicates(["id", "_ingested_at"])
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_year",  F.year("_ingested_at"))
        .withColumn("_month", F.month("_ingested_at"))
        .withColumn("_day",   F.dayofmonth("_ingested_at"))
        .select(
            "id", "name", "modeName",
            "statusSeverity", "statusSeverityDescription", "reason",
            "created", "modified", "_ingested_at",
            "_silver_processed_at", "_year", "_month", "_day"
        )
    )

    print(f"Silver records: {df_silver.count()}")
    df_silver.write.mode("overwrite").partitionBy("_year", "_month", "_day").parquet(SILVER_PATH)
    print(f"Done: {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
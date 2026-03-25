from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.bikepoints/"
SILVER_PATH = "s3a://tfl-silver-bucket/bikepoints/"

PROP_KEYS = ["TerminalName", "Installed", "Locked", "Temporary",
             "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes"]

def main():
    spark = SparkSession.builder.appName("bikepoint_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("inferSchema", "false").json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")

    prop_cols = [
        F.expr(f"filter(additionalProperties, x -> x.key = '{k}')[0].value").alias(k.lower())
        for k in PROP_KEYS
    ]

    df_silver = (
        df
        .select(
            F.col("id"),
            F.col("commonName"),
            F.col("placeType"),
            F.col("lat").cast(DoubleType()),
            F.col("lon").cast(DoubleType()),
            F.col("_ingested_at"),
            *prop_cols
        )
        .withColumn("nbbikes",         F.col("nbbikes").cast("integer"))
        .withColumn("nbemptydocks",    F.col("nbemptydocks").cast("integer"))
        .withColumn("nbdocks",         F.col("nbdocks").cast("integer"))
        .withColumn("nbstandardbikes", F.col("nbstandardbikes").cast("integer"))
        .withColumn("nbebikes",        F.col("nbebikes").cast("integer"))
        .withColumn("installed",       F.col("installed").cast("boolean"))
        .withColumn("locked",          F.col("locked").cast("boolean"))
        .withColumn("temporary",       F.col("temporary").cast("boolean"))
        .withColumn("_ingested_at",    F.to_timestamp("_ingested_at"))
        .filter(F.col("id").isNotNull())
        .dropDuplicates(["id", "_ingested_at"])
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
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.arrivals/"
SILVER_PATH = "s3a://tfl-silver-bucket/arrivals/"

SCHEMA = StructType([
    StructField("id",                   StringType(),  True),
    StructField("operationType",        IntegerType(), True),
    StructField("vehicleId",            StringType(),  True),
    StructField("naptanId",             StringType(),  True),
    StructField("stationName",          StringType(),  True),
    StructField("lineId",               StringType(),  True),
    StructField("lineName",             StringType(),  True),
    StructField("platformName",         StringType(),  True),
    StructField("direction",            StringType(),  True),
    StructField("destinationNaptanId",  StringType(),  True),
    StructField("destinationName",      StringType(),  True),
    StructField("timestamp",            StringType(),  True),
    StructField("timeToStation",        IntegerType(), True),
    StructField("currentLocation",      StringType(),  True),
    StructField("towards",              StringType(),  True),
    StructField("expectedArrival",      StringType(),  True),
    StructField("timeToLive",           StringType(),  True),
    StructField("modeName",             StringType(),  True),
    StructField("_ingested_at",         StringType(),  True),
    StructField("_source",              StringType(),  True),
    StructField("_topic",               StringType(),  True),
])

def main():
    spark = SparkSession.builder.appName("arrivals_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.schema(SCHEMA).json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")

    df_silver = (
        df
        .withColumn("timestamp",        F.to_timestamp("timestamp"))
        .withColumn("expectedArrival",  F.to_timestamp("expectedArrival"))
        .withColumn("timeToLive",       F.to_timestamp("timeToLive"))
        .withColumn("_ingested_at",     F.to_timestamp("_ingested_at"))
        .withColumn("stationName",      F.trim("stationName"))
        .withColumn("destinationName",  F.trim("destinationName"))
        .withColumn("currentLocation",  F.trim("currentLocation"))
        .withColumn("towards",          F.trim("towards"))
        .withColumn("direction",        F.lower(F.trim("direction")))
        .filter(F.col("id").isNotNull())
        .filter(F.col("naptanId").isNotNull())
        .filter(F.col("lineId").isNotNull())
        .dropDuplicates(["id", "timestamp"])
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_year",  F.year("timestamp"))
        .withColumn("_month", F.month("timestamp"))
        .withColumn("_day",   F.dayofmonth("timestamp"))
    )

    print(f"Silver records: {df_silver.count()}")
    df_silver.write.mode("overwrite").partitionBy("_year", "_month", "_day").parquet(SILVER_PATH)
    print(f"Done: {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
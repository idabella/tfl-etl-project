from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "s3a://tfl-bronze-bucket/topics/tfl.air_quality/"
SILVER_PATH = "s3a://tfl-silver-bucket/air_quality/"

def main():
    spark = SparkSession.builder.appName("airquality_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("inferSchema", "false").json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")

    df_silver = (
        df
        .withColumn("forecast", F.explode_outer(F.col("currentForecast")))
        .select(
            F.col("updatePeriod"),
            F.col("updateFrequency"),
            F.col("forecast.forecastType").alias("forecastType"),
            F.col("forecast.forecastID").alias("forecastId"),
            F.to_timestamp(F.col("forecast.fromDate")).alias("fromDate"),
            F.to_timestamp(F.col("forecast.toDate")).alias("toDate"),
            F.col("forecast.forecastBand").alias("forecastBand"),
            F.col("forecast.forecastSummary").alias("forecastSummary"),
            F.col("forecast.nO2Band").alias("no2Band"),
            F.col("forecast.o3Band").alias("o3Band"),
            F.col("forecast.pM10Band").alias("pm10Band"),
            F.col("forecast.pM25Band").alias("pm25Band"),
            F.col("forecast.sO2Band").alias("so2Band"),
            F.regexp_replace(
                F.regexp_replace(F.col("forecast.forecastText"), "<br/>", " "),
                "&lt;|&gt;|&#39;|&amp;", ""
            ).alias("forecastText"),
            F.col("_ingested_at"),
        )
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        .filter(F.col("forecastId").isNotNull())
        .dropDuplicates(["forecastId", "forecastType"])
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
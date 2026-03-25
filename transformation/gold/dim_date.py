from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType

GOLD_PATH = "s3a://tfl-gold-bucket/dimensions/dim_date/"

def main():
    spark = SparkSession.builder.appName("gold_dim_date").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_dates = (
        spark.range(0, 365 * 11)
        .withColumn("date", F.expr("date_add(to_date('2020-01-01'), cast(id as int))").cast(DateType()))
        .drop("id")
    )

    dim_date = (
        df_dates
        .withColumn("date_key",     F.date_format("date", "yyyyMMdd").cast("integer"))
        .withColumn("full_date",    F.col("date"))
        .withColumn("day_of_week",  F.dayofweek("date"))
        .withColumn("day_name",     F.date_format("date", "EEEE"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_year",  F.dayofyear("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("month_number", F.month("date"))
        .withColumn("month_name",   F.date_format("date", "MMMM"))
        .withColumn("month_short",  F.date_format("date", "MMM"))
        .withColumn("quarter",      F.quarter("date"))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("date")))
        .withColumn("year",         F.year("date"))
        .withColumn("year_month",   F.date_format("date", "yyyy-MM"))
        .withColumn("is_weekend",   F.dayofweek("date").isin([1, 7]))
        .withColumn("is_weekday",   ~F.dayofweek("date").isin([1, 7]))
        .withColumn("_created_at",  F.current_timestamp())
    )

    dim_date.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_date written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
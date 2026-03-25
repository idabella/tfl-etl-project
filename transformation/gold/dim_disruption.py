from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://tfl-silver-bucket/line_status/"
GOLD_PATH   = "s3a://tfl-gold-bucket/dimensions/dim_disruption/"

def main():
    spark = SparkSession.builder.appName("gold_dim_disruption").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)

    dim_disruption = (
        df.select("statusSeverity", "statusSeverityDescription")
        .dropDuplicates(["statusSeverity"])
        .withColumnRenamed("statusSeverity",            "severity_code")
        .withColumnRenamed("statusSeverityDescription", "severity_description")
        .withColumn("is_good_service",  F.col("severity_code") == 10)
        .withColumn("is_disrupted",     F.col("severity_code") < 10)
        .withColumn("severity_category",
            F.when(F.col("severity_code") == 10,           "Normal")
             .when(F.col("severity_code").between(6, 9),   "Minor")
             .when(F.col("severity_code").between(3, 5),   "Major")
             .otherwise("Severe"))
        .withColumn("disruption_key", F.monotonically_increasing_id().cast("integer"))
        .withColumn("_created_at",    F.current_timestamp())
    )

    dim_disruption.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_disruption written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
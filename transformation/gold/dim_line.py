from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "s3a://tfl-silver-bucket/line_status/"
GOLD_PATH   = "s3a://tfl-gold-bucket/dimensions/dim_line/"

LINE_COLORS = {
    "bakerloo": "#B36305", "central": "#E32017", "circle": "#FFD300",
    "district": "#00782A", "hammersmith-city": "#F3A9BB", "jubilee": "#A0A5A9",
    "metropolitan": "#9B0056", "northern": "#000000", "piccadilly": "#003688",
    "victoria": "#0098D4", "waterloo-city": "#95CDBA", "dlr": "#00A4A7",
    "elizabeth": "#6950A1", "london-overground": "#EE7C0E", "tfl-rail": "#0019A8",
    "bus": "#E1251B",
}

def main():
    spark = SparkSession.builder.appName("gold_dim_line").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)

    color_map = F.create_map(*[
        item for k, v in LINE_COLORS.items()
        for item in [F.lit(k), F.lit(v)]
    ])

    dim_line = (
        df.select("id", "name", "modeName")
        .dropDuplicates(["id"])
        .withColumnRenamed("id",       "line_id")
        .withColumnRenamed("name",     "line_name")
        .withColumnRenamed("modeName", "mode_name")
        .withColumn("line_color",    color_map[F.col("line_id")])
        .withColumn("is_tube",       F.col("mode_name") == "tube")
        .withColumn("is_bus",        F.col("mode_name") == "bus")
        .withColumn("is_dlr",        F.col("mode_name") == "dlr")
        .withColumn("is_overground", F.col("line_id").contains("overground"))
        .withColumn("line_key",      F.monotonically_increasing_id().cast("integer"))
        .withColumn("_created_at",   F.current_timestamp())
    )

    dim_line.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"dim_line written to {GOLD_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
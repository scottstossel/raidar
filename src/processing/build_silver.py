from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("Starting spark session...")

spark = (
    SparkSession.builder
    .appName("raidar-week2-inspect")
    .getOrCreate()
)

print("Spark session started.")

arxiv_df = spark.read.parquet("data/raw/arxiv/parquet")
github_df = spark.read.parquet("data/raw/github/parquet")

print("==== ARXIV SCHEMA ====")
arxiv_df.printSchema()

print("==== GITHUB SCHEMA ====")
github_df.printSchema()

print("==== ARXIV SAMPLE ====")
arxiv_df.show(5, truncate=False)

print("==== GITHUB SAMPLE ====")
github_df.show(5, truncate=False)

arxiv_with_ids = arxiv_df.withColumn(
    "arxiv_id",
    F.regexp_extract(F.col("id"), r"abs/(.+)$", 1)
)

arxiv_silver = arxiv_with_ids.select(
    F.col("source"),
    F.col("arxiv_id").alias("source_id"),
    F.concat(F.lit("arxiv:"), F.col("arxiv_id")).alias("doc_id"),
    F.col("title"),
    F.col("abstract").alias("body_text"),
    F.col("authors"),
    F.col("categories").alias("tags"),
    F.col("id").alias("url"),
    F.col("published_ts").alias("published_at"),
    F.col("updated_ts").alias("updated_at"),
    F.lit(None).cast("string").alias("language"),
    F.lit(None).cast("long").alias("stars"),
    F.col("ingested_at"),
)

print("==== ARXIV SILVER SCHEMA ====")
arxiv_silver.printSchema()

print("==== ARXIV SILVER SAMPLE ====")
arxiv_silver.show(5, truncate=False)

print("==== ARXIV SILVER SELECTED COLUMNS ====")
arxiv_silver.select("source_id", "doc_id", "url").show(5, truncate=False)
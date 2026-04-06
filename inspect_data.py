from pyspark.sql import SparkSession

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
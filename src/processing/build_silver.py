from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# setup spark session and read data
def create_spark():
    return (
        SparkSession.builder
        .appName("raidar-build-silver")
        .getOrCreate()
    )

def read_raw_data(spark):
    arxiv_df = spark.read.parquet("data/raw/arxiv/parquet")
    github_df = spark.read.parquet("data/raw/github/parquet")
    return arxiv_df, github_df

# build silver dataframes
def build_arxiv_silver(arxiv_df):
    arxiv_with_ids = arxiv_df.withColumn(
        "arxiv_id",
        F.regexp_extract(F.col("id"), r"abs/(.+)$", 1)
    )

    return arxiv_with_ids.select(
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

def build_github_silver(github_df):
    return github_df.select(
        F.col("source"),
        F.col("id").alias("source_id"),
        F.concat(F.lit("github:"), F.col("id")).alias("doc_id"),
        F.col("repo_name").alias("title"),
        F.col("description").alias("body_text"),
        F.lit(None).cast("array<string>").alias("authors"),
        F.col("topics").alias("tags"),
        F.col("html_url").alias("url"),
        F.col("created_at_ts").alias("published_at"),
        F.col("updated_at_ts").alias("updated_at"),
        F.col("language"),
        F.col("stargazers_count").alias("stars"),
        F.col("ingested_at"),
    )

def clean_silver_documents(df):
    return (
        df
        # trim text fields
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("body_text", F.trim(F.col("body_text")))

        # convert empty strings to null
        .withColumn(
            "title",
            F.when(F.col("title") == "", None).otherwise(F.col("title"))
        )
        .withColumn(
            "body_text",
            F.when(F.col("body_text") == "", None).otherwise(F.col("body_text"))
        )

        # drop rows with no usable content
        .filter(
            ~(F.col("title").isNull() & F.col("body_text").isNull())
        )
    )

# write

def write_silver(df):
    df.write.mode("overwrite").parquet("data/silver/documents")

def validate_output(spark):
    df = spark.read.parquet("data/silver/documents")

    print("==== FINAL COUNTS ====")
    print("Total: ", df.count())
    df.groupBy("source").count().show()

    print("==== Sample ====")
    df.select("source", "doc_id", "title").show(5, truncate=False)

def main():
    spark = create_spark()

    # read raw data
    arxiv_df, github_df = read_raw_data(spark)
    
    # transform to silver
    arxiv_silver = build_arxiv_silver(arxiv_df)
    github_silver = build_github_silver(github_df)

    # combine
    silver_documents = arxiv_silver.unionByName(github_silver)

    print("==== BEFORE CLEANING ====")
    silver_documents.groupBy("source").count().show()

    # clean
    silver_clean = clean_silver_documents(silver_documents)

    print("==== AFTER CLEANING ====")
    print("Total: ", silver_clean.count())
    silver_clean.groupBy("source").count().show()

    # write output
    write_silver(silver_clean)

    # validate
    validate_output(spark)

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def read_parquet(spark, path):
    return spark.read.parquet(path)

def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def build_searchable_text(df):
    return F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title"), F.lit("")),
            F.coalesce(F.col("body_text"), F.lit("")),
            F.coalesce(F.concat_ws(" ", F.col("tags")), F.lit("")),
        )
    )

def filter_by_keywords(df, keywords):
    searchable_text = build_searchable_text(df)

    condition = None
    for keyword in keywords:
        keyword_condition = searchable_text.contains(keyword.lower())
        condition = keyword_condition if condition is None else (condition | keyword_condition)

    return df.filter(condition)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark():
    return (
        SparkSession.builder
        .appName("raidar-build-silver")
        .getOrCreate()
    )

def read_silver(spark):
    return spark.read.parquet("data/silver/documents")

def filter_ai_documents(df):
    ai_keywords = [
        "ai", "ml", "llm", "language model", "transformer", "transformers", "diffusion", "neural network",
        "neural networks", "deep learning", "machine learning", "vision transformer", "reinforcement learning",
        "multimodal", "foundation model", "foundation models", "generative"
    ]

    searchable_text = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title"), F.lit("")),
            F.coalesce(F.col("body_text"), F.lit("")),
            F.coalesce(F.concat_ws(" ", F.col("tags")), F.lit(""))
        )
    )

    condition = None
    for keyword in ai_keywords:
        keyword_condition = searchable_text.contains(keyword)
        condition = keyword_condition if condition is None else (condition | keyword_condition)

    return df.filter(condition)

def write_ai(df):
    df.write.mode("overwrite").parquet("data/silver/ai_documents")

def main():
    spark = create_spark()

    silver_df = read_silver(spark)
    ai_df = filter_ai_documents(silver_df)

    print("==== AI DOCUMENT COUNTS ====")
    print("Total: ", ai_df.count())
    ai_df.groupBy("source").count().show()

    write_ai(ai_df)

if __name__ == "__main__":
    main()
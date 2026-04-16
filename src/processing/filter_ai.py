from utils import create_spark, read_parquet, write_parquet, filter_by_keywords

AI_KEYWORDS = [
    "ai",
    "llm",
    "machine learning",
    "deep learning",
    "transformer",
    "multimodal",
]

def main():
    spark = create_spark("raidar-filter-ai")
    df = read_parquet(spark, "data/silver/documents")
    filtered = filter_by_keywords(df, AI_KEYWORDS)

    print("==== AI DOCUMENT COUNTS ====")
    print("Total:", filtered.count())
    filtered.groupBy("source").count().show()

    write_parquet(filtered, "data/silver/ai_documents")

if __name__ == "__main__":
    main()
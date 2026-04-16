from utils import create_spark, read_parquet, write_parquet, filter_by_keywords

RAIDAR_KEYWORDS = [
    "evaluation",
    "eval",
    "benchmark",
    "benchmarking",
    "testing",
    "metric",
    "judge",
    "safety",
    "alignment",
    "robustness",
    "reliability",
    "hallucination",
    "guardrail",
    "monitoring",
    "agent",
    "agents",
    "multi-agent",
    "autonomous",
    "orchestration",
    "tool use",
]

def main():
    spark = create_spark("raidar-filter")
    df = read_parquet(spark, "data/silver/documents")
    filtered = filter_by_keywords(df, RAIDAR_KEYWORDS)

    print("==== RAIDAR DOCUMENT COUNTS ====")
    print("Total:", filtered.count())
    filtered.groupBy("source").count().show()

    write_parquet(filtered, "data/silver/raidar_documents")

    reloaded = read_parquet(spark, "data/silver/raidar_documents")
    print("==== RELOADED RAIDAR COUNTS ====")
    print("Total:", reloaded.count())
    reloaded.groupBy("source").count().show()

    print("==== RAIDAR SAMPLE ====")
    reloaded.select("source", "doc_id", "title").show(5, truncate=False)

if __name__ == "__main__":
    main()

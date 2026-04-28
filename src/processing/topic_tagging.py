from pyspark.sql import functions as F
from src.processing.topic_config import TOPIC_KEYWORDS

def add_analysis_text(df):
    return df.withColumn(
        "analysis_text",
        F.lower(
            F.concat_ws(
                " ",
                F.coalesce(F.col("title"), F.lit("")),
                F.coalesce(F.col("body_text"), F.lit("")),
                F.coalesce(F.col("source"), F.lit("")),
            )
        )
    )

def add_topic_flags(df):
    for topic, keywords in TOPIC_KEYWORDS.items():
        pattern = "|".join([rf"\\b{k.lower()}\\b" for k in keywords])
        df = df.withColumn(
            f"is_{topic}",
            F.col("analysis_text").rlike(pattern)
        )
    return df

def add_topic_tags(df):
    return df.withColumn(
        "topic_tags",
        F.array_remove(
            F.array(
                F.when(F.col("is_llm_eval"), F.lit("llm_eval")),
                F.when(F.col("is_ai_safety"), F.lit("ai_safety")),
                F.when(F.col("is_agents"), F.lit("agents")),
            ),
            F.lit(None)
        )
    )

def tag_documents(df):
    df = add_analysis_text(df)
    df = add_topic_flags(df)
    df = add_topic_tags(df)
    return df
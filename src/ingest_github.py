from __future__ import annotations

import os
from typing import Any

import requests
import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import get_settings
from logging_utils import get_logger
from spark_utils import get_spark

logger = get_logger(__name__)
GITHUB_URL = "https://api.github.com/search/repositories"

def fetch_github_raw(query: str, max_results: int, token: str) -> dict[str, Any]:
    headers = {
        "Accept": "application/vnd.github.v3+json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params = {
        "q": query,
        "sort": "updated",
        "order": "desc",
        "per_page": min(max_results, 100),
    }

    response = requests.get(GITHUB_URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def parse_github_response(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for item in payload.get("items", []):
        record = {
            "source": "github",
            "id": str(item.get("id", "")),
            "repo_name": item.get("name", ""),
            "full_name": item.get("full_name", ""),
            "description": item.get("description", ""),
            "html_url": item.get("html_url", ""),
            "language": item.get("language", ""),
            "stargazers_count": item.get("stargazers_count", 0),
            "forks_count": item.get("forks_count", 0),
            "open_issues_count": item.get("open_issues_count", 0),
            "created_at": item.get("created_at", ""),
            "updated_at": item.get("updated_at", ""),
            "topics": item.get("topics", []),
        }
        records.append(record)

    return records

def records_to_spark_df(records: list[dict[str, Any]]) -> DataFrame:
    spark = get_spark("raidar-github-ingestion")
    df = spark.createDataFrame(records)

    df = (
        df
            .withColumn("created_at_ts", F.to_timestamp("created_at"))
            .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
            .withColumn("ingested_at", F.current_timestamp())
    )

    return df

def save_github_outputs(payload: dict[str, Any], df: DataFrame, base_dir: str) -> None:
    raw_json_dir = os.path.join(base_dir, "raw", "github", "json")
    raw_parquet_dir = os.path.join(base_dir, "raw", "github", "parquet")

    os.makedirs(raw_json_dir, exist_ok=True)
    os.makedirs(raw_parquet_dir, exist_ok=True)

    json_path = os.path.join(raw_json_dir, "github_raw.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    df.write.mode("overwrite").parquet(raw_parquet_dir)

def run() -> None:
    settings = get_settings()

    logger.info("Fetching GitHub repository data")
    payload = fetch_github_raw(
        query=settings.github_search_query,
        max_results=settings.github_max_results,
        token=settings.github_token,
    )

    logger.info("Parsing GitHub payload into Python records")
    records = parse_github_response(payload)
    logger.info("Parsed %s GitHub records", len(records))

    logger.info("Creating Spark DataFrame")
    df = records_to_spark_df(records)

    logger.info("GitHub schema:")
    df.printSchema()

    logger.info("Sample GitHub rows")
    df.show(5, truncate=False)

    logger.info("Saving GitHub raw outputs")
    save_github_outputs(payload, df, settings.data_dir)

    logger.info("Finished GitHub ingestion")

if __name__ == "__main__":
    run()
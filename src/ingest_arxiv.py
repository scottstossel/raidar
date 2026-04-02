from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from typing import Any

import requests
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import get_settings
from logging_utils import get_logger
from spark_utils import get_spark

logger = get_logger(__name__)

ARXIV_URL = "http://export.arxiv.org/api/query"

def fetch_arxiv_raw(max_results: int) -> str:
    params = {
        "search_query": "cat:cs.AI OR cat:cs.LG OR all:safety OR all:evaluation OR all:agent",
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    response = requests.get(ARXIV_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.text

def parse_arxiv_xml(xml_text: str) -> list[dict[str, Any]]:
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }

    root = ET.fromstring(xml_text)
    records: list[dict[str, Any]] = []

    for entry in root.findall("atom:entry", ns):
        authors = [
            author.findtext("atom:name", default="", namespaces=ns)
            for author in entry.findall("atom:author", ns)
        ]

        categories = [
            cat.attrib.get("term", "")
            for cat in entry.findall("atom:category", ns)
        ]

        record = {
            "source": "arxiv",
            "id": entry.findtext("atom:id", default="", namespaces=ns),
            "title": entry.findtext("atom:title", default="", namespaces=ns).strip(),
            "abstract": entry.findtext("atom:summary", default="", namespaces=ns).strip(),
            "published": entry.findtext("atom:published", default="", namespaces=ns),
            "updated": entry.findtext("atom:updated", default="", namespaces=ns),
            "authors": authors,
            "categories": categories,
        }
        records.append(record)

    return records

def records_to_spark_df(records: list[dict[str, Any]]) -> DataFrame:
    spark = get_spark("raidar-arxiv-ingestion")
    df = spark.createDataFrame(records)

    df = (
        df.withColumn("published_ts", F.to_timestamp("published"))
            .withColumn("updated_ts", F.to_timestamp("updated"))
            .withColumn("ingested_at", F.current_timestamp())
    )
    
    return df

def save_arxiv_outputs(xml_text: str, df: DataFrame, base_dir: str) -> None:
    raw_json_dir = os.path.join(base_dir, "raw", "arxiv", "xml")
    raw_parquet_dir = os.path.join(base_dir, "raw", "arxiv", "parquet")

    os.makedirs(raw_json_dir, exist_ok=True)
    os.makedirs(raw_parquet_dir, exist_ok=True)

    xml_path = os.path.join(raw_json_dir, "arxiv_raw.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_text)

    df.write.mode("overwrite").parquet(raw_parquet_dir)

def run() -> None:
    settings = get_settings()

    logger.info("Fetching arXiv data")
    xml_text = fetch_arxiv_raw(settings.arxiv_max_results)

    logger.info("Parsing arXiv XML into Python records")
    records = parse_arxiv_xml(xml_text)
    logger.info("Parsed %s arXiv records", len(records))

    logger.info("Creating Spark DataFrame")
    df = records_to_spark_df(records)

    logger.info("arXiv schema:")
    df.printSchema()

    logger.info("Sample arXiv rows")
    df.show(5, truncate=False)

    logger.info("Saving arXiv raw outputs")
    save_arxiv_outputs(xml_text, df, settings.data_dir)

    logger.info("Finished arXiv ingestion")

if __name__ == "__main__":
    run()
"""
Daily RAIDAR pipeline: ingest → features → intel → brief

Runs daily at 00:00 UTC. Ingests from all sources, computes features,
runs multi-agent reasoning, and generates a daily brief.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "raidar",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "raidar_daily_pipeline",
    default_args=default_args,
    description="Daily RAIDAR ingestion, feature, and intel pipeline",
    schedule_interval="0 0 * * *",  # Daily at 00:00 UTC
    catchup=False,
)


def run_ingestion():
    """Run ingestion from all sources."""
    from src.ingestion.run import run_full_ingestion
    results = run_full_ingestion()
    total = sum(results.values())
    print(f"Ingestion complete: {total} new documents")
    for source, count in results.items():
        print(f"  {source}: {count} new")


def run_feature_computation():
    """Compute features for newly ingested documents."""
    from src.features.run import compute_features_for_new_documents
    count = compute_features_for_new_documents(batch_size=100)
    print(f"Feature computation complete: {count} documents processed")


def run_intel_layer():
    """Run multi-agent reasoning on documents."""
    # TODO: Implement intel layer
    print("Intel layer not yet implemented")


def run_synthesis():
    """Generate daily brief."""
    # TODO: Implement synthesis
    print("Synthesis not yet implemented")


with dag:
    # Task 1: Ingest from all sources
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=run_ingestion,
        pool="ingestion_pool",
    )

    # Task 2: Compute features
    features_task = PythonOperator(
        task_id="compute_features",
        python_callable=run_feature_computation,
        pool="feature_pool",
    )

    # Task 3: Run intel layer
    intel_task = PythonOperator(
        task_id="run_intel",
        python_callable=run_intel_layer,
        pool="intel_pool",
    )

    # Task 4: Generate brief
    synthesis_task = PythonOperator(
        task_id="synthesize_brief",
        python_callable=run_synthesis,
        pool="synthesis_pool",
    )

    # Define dependencies
    ingest_task >> features_task >> intel_task >> synthesis_task

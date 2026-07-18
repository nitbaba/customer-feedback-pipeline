import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.path.expanduser("~/repos/customer-feedback-pipeline")
SILVER_DATA_DIR = os.path.join(PROJECT_ROOT, "data_lake/silver")
ANALYTICS_JOB_PATH = os.path.join(PROJECT_ROOT, "src/jobs/historical_analytics.py")
SILVER_JOB_PATH = os.path.join(PROJECT_ROOT, "src/jobs/stream_to_silver.py")
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv/bin/python3")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retries_delay": timedelta(minutes=2),
}

with DAG(
    "customer_feedback_medallion_pipeline",
    default_args=default_args,
    description="Automated Medallion architecture processing chain",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "analytics", "spark"],
) as dag:

    # Task 1: Explicitly process the micro-batch ingestion from Bronze to Silver
    run_silver_stream_batch = BashOperator(
        task_id="execute_silver_stream_sync",
        bash_command=f"{VENV_PYTHON} {SILVER_JOB_PATH}"
    )

    # Task 2: Calculate Gold layer metrics and upsert changes to PostgreSQL
    run_gold_batch_job = BashOperator(
        task_id="execute_historical_analytics_sync",
        bash_command=f"{VENV_PYTHON} {ANALYTICS_JOB_PATH}",
        # FIX: Explicitly inject environment variables to ensure the worker connects flawlessly
        env={
            "AWS_DEFAULT_REGION": "us-east-1",
            "ENVIRONMENT": "dev"
        }
    )

    pipeline_execution_complete = BashOperator(
        task_id="log_pipeline_completion_status",
        bash_command="echo 'Medallion pipeline execution completed and synced successfully.'"
    )

    run_silver_stream_batch >> run_gold_batch_job >> pipeline_execution_complete
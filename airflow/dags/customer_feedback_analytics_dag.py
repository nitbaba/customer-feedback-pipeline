import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.path.expanduser("~/repos/customer-feedback-pipeline")
SILVER_DATA_DIR = os.path.join(PROJECT_ROOT, "data_lake/silver")
ANALYTICS_JOB_PATH = os.path.join(PROJECT_ROOT, "src/jobs/historical_analytics.py")
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv/bin/python3")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retries_delay": timedelta(minutes=2),
}

def check_silver_layer_has_data():
    """
    Ensures silver layer dir exists and contains data before boot Spark cluster
    """
    if not os.path.exists(SILVER_DATA_DIR):
        raise FileNotFoundError(f"\nMissing data path: {SILVER_DATA_DIR}\n")

    data_files = [f for f in os.listdir(SILVER_DATA_DIR) if f.endswith(".parquet")]
    if len(data_files) == 0:
        raise ValueError(f"\n{SILVER_DATA_DIR} is empty.\n")
    
    print(f"Found {len(data_files)} Parquet files.")

with DAG(
    "customer_feedback_medallion_pipeline",
    default_args=default_args,
    description="Automated Gold tier historical anlytics batch sync pipeline",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "analytics", "spark"],
) as dag:

    verify_silver_data = PythonOperator(
        task_id="verify_silver_data_presence",
        python_callable=check_silver_layer_has_data,
    )

    run_gold_batch_job = BashOperator(
        task_id="execute_historical_analytics_sync",
        bash_command=f"{VENV_PYTHON} {ANALYTICS_JOB_PATH}",
    )

    pipeline_execution_complete = BashOperator(
        task_id="log_pipeline_completion_status",
        bash_command="echo 'Medallion pipeline execution completed and synced success.'"
    )

    verify_silver_data >> run_gold_batch_job >> pipeline_execution_complete
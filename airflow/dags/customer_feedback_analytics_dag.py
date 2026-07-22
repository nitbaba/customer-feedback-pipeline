import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.task_group import TaskGroup

S3_CODE_BUCKET = "s3://customer-feedback-pipeline-dev-lake"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Spark Step Definitions for the EMR Cluster
SPARK_STEPS = [
    {
        "Name": "Execute Silver Stream Sync",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"{S3_CODE_BUCKET}/jobs/stream_to_silver.py",
            ],
        },
    },
    {
        "Name": "Execute Historical Analytics Sync",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"{S3_CODE_BUCKET}/jobs/historical_analytics.py",
            ],
        },
    },
]

with DAG(
    "customer_feedback_medallion_pipeline",
    default_args=default_args,
    description="Automated Medallion architecture processing chain",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "analytics", "spark"],
) as dag:

    with TaskGroup(
        group_id="emr_analytics_processing",
        tooltip="Submit and monitor Spark steps on active EMR cluster",
    ) as emr_analytics_processing:

        # Dynamically add processing steps to your running EMR cluster
        add_analytics_steps = EmrAddStepsOperator(
            task_id="add_analytics_steps",
            job_flow_id="{{ var.value.get('active_emr_cluster_id') }}",
            steps=SPARK_STEPS,
            aws_conn_id="aws_default",
        )

        # Watch the second step (Historical Analytics Sync) to guarantee execution completes
        watch_analytics_steps = EmrStepSensor(
            task_id="watch_analytics_steps",
            job_flow_id="{{ var.value.get('active_emr_cluster_id') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='emr_analytics_processing.add_analytics_steps')[1] }}",
            aws_conn_id="aws_default",
        )

        add_analytics_steps >> watch_analytics_steps
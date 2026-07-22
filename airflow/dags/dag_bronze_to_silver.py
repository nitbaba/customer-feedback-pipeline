from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.models import Variable

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch cluster configuration (resolved dynamically via Secrets Manager Backend or Airflow Vars)
EMR_CLUSTER_ID = Variable.get("EMR_CLUSTER_ID", default_var=os.getenv("EMR_CLUSTER_ID"))
S3_CODE_BUCKET = Variable.get("S3_CODE_BUCKET", default_var=os.getenv("S3_CODE_BUCKET"))
S3_DATA_BUCKET = Variable.get("S3_DATA_BUCKET", default_var=os.getenv("S3_DATA_BUCKET"))

SPARK_STEPS = [
    {
        "Name": "Medallion Bronze to Silver Streaming Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                # Pass S3 buckets & flags directly to PySpark job execution
                "--conf",
                f"spark.executorEnv.BRONZE_S3_PATH=s3://{S3_DATA_BUCKET}/bronze/",
                "--conf",
                f"spark.executorEnv.SILVER_S3_PATH=s3://{S3_DATA_BUCKET}/silver/",
                "--conf",
                f"spark.executorEnv.CHECKPOINT_S3_PATH=s3://{S3_DATA_BUCKET}/checkpoints/bronze_to_silver/",
                "--conf",
                "spark.executorEnv.USE_AWS_SECRETS=true",
                f"s3://{S3_CODE_BUCKET}/jobs/stream_to_silver.py",
            ],
        },
    }
]

with DAG(
    dag_id="medallion_bronze_to_silver",
    default_args=default_args,
    description="Orchestrates PySpark Bronze-to-Silver ETL on AWS EMR 7.1.0",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "emr", "bronze-to-silver", "pyspark"],
) as dag:

    # Task 1: Submit PySpark job step to EMR Cluster
    add_step = EmrAddStepsOperator(
        task_id="add_pyspark_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    # Task 2: Sensor to wait for step execution to complete
    wait_for_step = EmrStepSensor(
        task_id="wait_for_pyspark_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_pyspark_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=3600,
    )

    add_step >> wait_for_step
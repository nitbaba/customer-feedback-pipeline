from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

S3_CODE_BUCKET = "s3://customer-feedback-pipeline-dev-lake"
EMR_EC2_ROLE = "EMR_EC2_DefaultRole"
EMR_ROLE = "EMR_DefaultRole"

JOB_FLOW_OVERRIDES = {
    "Name": "Transient_Medallion_Streaming_Cluster",
    "ReleaseLabel": "emr-7.1.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": EMR_EC2_ROLE,
    "ServiceRole": EMR_ROLE,
}

SPARK_STEPS = [
    {
        "Name": "Run Stream to Bronze",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                f"{S3_CODE_BUCKET}/jobs/stream_to_bronze.py"
            ],
        },
    },
    {
        "Name": "Run Stream to Silver",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                f"{S3_CODE_BUCKET}/jobs/stream_to_silver.py"
            ],
        },
    },
]

default_args = {
    "owner": "devops-data-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "emr_medallion_stream_orchestration",
    default_args=default_args,
    description="Orchestrate transient EMR clusters for Medallion Spark architecture",
    schedule_interval="@daily",
    catchup=False,
    tags=["production", "emr", "medallion"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    add_processing_steps = EmrAddStepsOperator(
        task_id="add_processing_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    watch_emr_cluster = EmrJobFlowSensor(
        task_id="watch_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,
    )

    create_emr_cluster >> add_processing_steps >> watch_emr_cluster
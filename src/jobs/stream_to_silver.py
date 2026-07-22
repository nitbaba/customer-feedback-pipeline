import json
import logging
import os
import sys
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.llm_triage import call_mock_llm  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream_to_silver")


def get_secret(secret_name: str, region_name: str = "us-east-1") -> dict:
    """
    Retrieves secret key-value pairs from AWS Secrets Manager dynamically at runtime.
    """
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        logger.info(f"Fetching secret: {secret_name}")
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise e

    if "SecretString" in response:
        return json.loads(response["SecretString"])
    else:
        raise ValueError(f"Secret {secret_name} does not contain a valid SecretString.")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))

    # Configurable paths with S3 fallback support via env vars
    src_dir = os.path.join(project_root, "src")
    bronze_dir = os.getenv("BRONZE_S3_PATH", os.path.join(project_root, "data_lake", "bronze"))
    silver_dir = os.getenv("SILVER_S3_PATH", os.path.join(project_root, "data_lake", "silver"))
    checkpoint_dir = os.getenv(
        "CHECKPOINT_S3_PATH",
        os.path.join(project_root, "data_lake", "checkpoints", "silver"),
    )

    # Fetch secrets from AWS Secrets Manager if configured (e.g., in AWS environment)
    secret_name = os.getenv("AWS_SECRET_NAME", "medallion/rds/postgres")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if os.getenv("USE_AWS_SECRETS", "false").lower() == "true":
        try:
            db_credentials = get_secret(secret_name=secret_name, region_name=aws_region)
            logger.info("Successfully fetched connection details from AWS Secrets Manager.")
        except Exception as err:
            logger.warning(f"Could not fetch secret '{secret_name}': {err}")

    logger.info("Initializing Spark session...")
    spark_builder = SparkSession.builder.appName("CustomerFeedbackSilverStreaming")

    # If running locally without EMR cluster manager
    if os.getenv("SPARK_ENV", "local") == "local":
        spark_builder = spark_builder.master("local[4]")

    spark = (
        spark_builder
        .config("spark.pyspark.driver.acceptConnIfUntrusted", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sys.path.append(src_dir)
    spark.sparkContext.addPyFile(os.path.join(src_dir, "utils", "llm_triage.py"))

    import llm_triage

    call_mock_llm_fn = llm_triage.call_mock_llm

    bronze_schema = StructType(
        [
            StructField("ticket_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("review_text", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
        ]
    )

    llm_output_schema = StructType(
        [
            StructField("sentiment", StringType(), True),
            StructField("urgency_score", IntegerType(), True),
            StructField("summary", StringType(), True),
        ]
    )

    @udf(returnType=StringType())
    def serialize_llm_triage_udf(reviews: pd.Series) -> pd.Series:
        return reviews.apply(call_mock_llm_fn)

    logger.info(f"Monitoring Bronze storage tier: {bronze_dir}")

    bronze_stream_df = (
        spark.readStream.format("parquet").schema(bronze_schema).load(bronze_dir)
    )

    silver_enriched_df = bronze_stream_df.withColumn(
        "raw_llm_json", serialize_llm_triage_udf(col("review_text"))
    )

    silver_final_df = silver_enriched_df.withColumn(
        "llm_data", from_json(col("raw_llm_json"), llm_output_schema)
    ).select(
        col("ticket_id"),
        col("customer_id"),
        col("product_id"),
        col("review_text"),
        col("llm_data.sentiment").alias("sentiment"),
        col("llm_data.urgency_score").alias("urgency_score"),
        col("llm_data.summary").alias("summary"),
        col("ingested_at").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at"),
    )

    logger.info(f"Directing enriched stream output to Silver tier: {silver_dir}")

    query = (
        silver_final_df.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start(silver_dir)
    )

    try:
        query.awaitTermination()
        logger.info("Silver micro-batch execution completed successfully.")
        spark.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Stream tracking execution failed: {str(e)}")
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Configure structured logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_secret(secret_name: str, region_name: str = "us-east-1") -> dict:
    """Retrieve database credentials or configuration from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise e

    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    return {}


def main():
    use_aws_secrets = os.getenv("USE_AWS_SECRETS", "false").lower() == "true"
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bronze_path = os.getenv("BRONZE_S3_PATH", "data_lake/bronze/")
    silver_path = os.getenv("SILVER_S3_PATH", "data_lake/silver/")
    checkpoint_path = os.getenv(
        "CHECKPOINT_S3_PATH", "data_lake/checkpoints/bronze_to_silver/"
    )

    # Initialize secret variables with local development fallbacks
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "medallion")

    # Fetch secrets dynamically if running in production cloud context
    if use_aws_secrets:
        secret_name = "medallion/rds/postgres"
        db_credentials = get_secret(secret_name, aws_region)

        # Extract values directly from AWS Secrets Manager dictionary
        db_user = db_credentials.get("username", db_user)
        db_password = db_credentials.get("password", db_password)
        db_host = db_credentials.get("host", db_host)
        db_port = db_credentials.get("port", db_port)
        db_name = db_credentials.get("dbname", db_name)

    # Construct JDBC Connection URL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    spark = (
        SparkSession.builder.appName("Medallion-BronzeToSilver-Streaming")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    # Example: Enriching streaming data using a static lookup table from RDS PostgreSQL via JDBC
    user_metadata_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.user_metadata")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    raw_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("payload", StringType(), True),
        ]
    )

    bronze_stream = (
        spark.readStream.format("json")
        .schema(raw_schema)
        .option("maxFilesPerTrigger", 100)
        .load(bronze_path)
    )

    # Stream transformation enriched with RDS PostgreSQL data
    silver_transformed = (
        bronze_stream.filter(col("event_id").isNotNull())
        .join(user_metadata_df, on="user_id", how="left")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))
        .drop("timestamp")
    )

    query = (
        silver_transformed.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_path)
        .option("path", silver_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    logging.info("Bronze-to-Silver batch complete.")


if __name__ == "__main__":
    main()
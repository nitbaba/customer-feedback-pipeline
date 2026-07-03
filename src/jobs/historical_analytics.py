import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when

def resolve_credentials():
    """
    Dual-layer credential resolver
        Layer 1 (Dev): Checks local sys env vars
        Layer 2 (Prod): Falls back to AWS Secrets Manager API using active IAM context
    """
    #Layer 1
    if "DB_ENDPOINT" in os.environ and "DB_PASSWORD" in os.environ:
        print(f"\nLocal env vars detected.")
        return os.environ["DB_ENDPOINT"], os.environ["DB_PASSWORD"]

    #Layer 2
    print(f"Local env vars missing. Querying AWS Secrets manager")
    secret_name = "customer-feedback-pipeline-dev-db-credentials"
    region_name = "us-east-1"

    #Standard AWS CLI credential chain, from aws configure
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict["endpoint"], secret_dict["password"]
    except ClientError as e:
        print(f"Error: Unable to resolve credentials via Local Env or AWS API")
        print(f"Root cause: {e}")
        sys.exit(1)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    silver_dir = os.path.join(project_root, "data_lake", "silver")

    rds_endpoint, db_password = resolve_credentials()

    #RDS_ENDPOINT = ""
    #DB_PASSWORD = ""

    print(f"\nInitializing Gold analytics Spark engine with JDBC support.\n")
    spark = (SparkSession.builder
             .appName("CustomerFeedbackGoldBatch")
             .master("local[4]")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nReading transformed data from Silver layer: {silver_dir}")
    if not os.path.exists(silver_dir) or len(os.listdir(silver_dir)) == 0:
        print(f"\nSilver layer empty. Run streaming job to populate.")
        spark.stop()
        return

    silver_df = spark.read.parquet(silver_dir)

    print(f"\nRolling historical aggregates aka Gold tier.")

    gold_metrics_df = (silver_df
                        .groupBy("product_id")
                        .agg(
                            count("ticket_id").alias("total_reviews"),
                            avg("urgency_score").alias("avg_urgency_rating"),
                            count(when(col("sentiment") == "Negative", 1)).alias("negative_ticket_count"),
                            count(when(col("sentiment") == "Positive", 1)).alias("positive_ticket_count")
                        ))
    
    print(f"\nSyncing Gold metrics to AWS RDS PostgreSQL instance: {rds_endpoint}")

    jdbc_url = f"jdbc:postgresql://{rds_endpoint}/feedback_analytics"
    connection_properties = {
        "user": "pipeline_admin",
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    optimized_gold_df = gold_metrics_df.coalesce(1)

    #Enforce Idempotency: Overwrite previous batch evals to avoid deuplication errors
    (optimized_gold_df.write
     .jdbc(url=jdbc_url, table="product_performance_metrics", mode="overwrite", properties=connection_properties))

    print(f"\n Gold layer metrics successfully published. Ending cluster compute safely.")
    spark.stop()

if __name__ == "__main__":
    main()
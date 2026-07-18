import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import psycopg2

def get_db_credentials():
    """
    Resolves database credentials by prioritizing local environment variables,
    falling back to AWS Secrets Manager if they are missing.
    """
    endpoint = os.getenv("DB_ENDPOINT")
    password = os.getenv("DB_PASSWORD")
    username = os.getenv("DB_USER", "pipeline_admin")
    database = os.getenv("DB_NAME", "feedback_analytics")

    if endpoint and password:
        print("Using database credentials from local environment variables.")
        return username, password, endpoint, database

    print("Local env vars missing. Querying AWS Secrets Manager...")
    secret_name = "customer-feedback-pipeline-dev-db-credentials"
    region_name = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])
        
        # Clean endpoint string to remove port numbers if appended by Terraform exports
        db_endpoint = secret["endpoint"].split(":")[0]
        return secret["username"], secret["password"], db_endpoint, database
    except Exception as e:
        print(f"Failed to retrieve secrets from AWS Secrets Manager: {str(e)}")
        raise e

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    silver_dir = os.path.join(project_root, "data_lake", "silver")

    # 1. Resolve connectivity properties
    db_user, db_password, db_endpoint, db_name = get_db_credentials()
    jdbc_url = f"jdbc:postgresql://{db_endpoint}:5432/{db_name}"

    print("\nInitializing Gold analytics Spark engine with JDBC support.")
    spark = (SparkSession.builder
             .appName("CustomerFeedbackGoldBatch")
             .master("local[4]")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 2. Extract dataset from Silver parquet tier
    print(f"\nReading transformed data from Silver layer: {silver_dir}")
    if not os.path.exists(silver_dir) or not os.listdir(silver_dir):
        print("Silver directory is empty or missing. Exiting batch job early.")
        spark.stop()
        sys.exit(0)

    silver_df = spark.read.parquet(silver_dir)

    # 3. Compute Gold aggregates
    print("\nRolling historical aggregates aka Gold tier.")
    gold_metrics_df = (silver_df
                       .groupBy("product_id")
                       .agg(
                           count("ticket_id").alias("total_reviews"),
                           avg("urgency_score").alias("avg_urgency")
                       ))

    # 4. Write data to a staging table in PostgreSQL using Spark JDBC
    staging_table = "product_performance_metrics_staging"
    target_table = "product_performance_metrics"

    print(f"\nSyncing Gold metrics staging records to AWS RDS PostgreSQL: {db_endpoint}")
    (gold_metrics_df.write
     .format("jdbc")
     .option("url", jdbc_url)
     .option("dbtable", f"public.{staging_table}")
     .option("user", db_user)
     .option("password", db_password)
     .option("driver", "org.postgresql.Driver")
     .mode("overwrite")
     .save())

    # 5. Execute self-healing schema patch and transaction upsert via psycopg2
    try:
        conn = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password,
            port=5432
        )
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            print("Verifying target table structure and repairing schema drifts...")
            
            # Self-healing Check A: Create target table if an upstream Spark overwrite erased it
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS public.{target_table} (
                    product_id VARCHAR(255) PRIMARY KEY,
                    total_reviews BIGINT,
                    avg_urgency DOUBLE PRECISION,
                    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Self-healing Check B: Inject tracking column back if dropped by previous jobs
            cursor.execute(f"""
                ALTER TABLE public.{target_table} 
                ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            """)
            conn.commit()

            # Execute transactional Upsert from staging back to main metrics target
            upsert_sql = f"""
            INSERT INTO public.{target_table} (
                product_id, 
                total_reviews, 
                avg_urgency, 
                last_updated_at
            )
            SELECT 
                product_id, 
                total_reviews, 
                avg_urgency, 
                CURRENT_TIMESTAMP
            FROM public.{staging_table}
            ON CONFLICT (product_id) 
            DO UPDATE SET 
                total_reviews = EXCLUDED.total_reviews,
                avg_urgency = EXCLUDED.avg_urgency,
                last_updated_at = CURRENT_TIMESTAMP;
            """

            print("Performing UPSERT operation from staging layer.")
            cursor.execute(upsert_sql)
            
            # Clean up the staging layer table to optimize database storage space
            cursor.execute(f"DROP TABLE IF EXISTS public.{staging_table};")
            
            conn.commit()
            print("Gold layer metrics synced flawlessly and staging elements cleared.")

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        print(f"Upsert transaction processing failed: {str(e)}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
import os
import json
import boto3
import psycopg2
from botocore.exceptions import ClientError

def get_db_credentials():
    if "DB_ENDPOINT" in os.environ and "DB_PASSWORD" in os.environ:
        return os.environ["DB_ENDPOINT"], os.environ["DB_PASSWORD"]

    secret_name = "customer-feedback-pipeline-dev-db-credentials"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret["endpoint"], secret["password"]

def run_migrations():
    endpoint, password = get_db_credentials()
    host, port = endpoint.split(":")

    print(f"\nConnecting to realtional sink at {host}")

    conn = psycopg2.connect(
        host=host,
        port=port,
        user="pipeline_admin",
        password=password,
        database="postgres"
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 from pg_catalog.pg_database WHERE datname = 'feedback_analytics';")
    if not cursor.fetchone():
        print(f"\nCreating target databse: feedback_analytics")
        cursor.execute("CREATE DATABASE feedback_analytics;")

    cursor.close()
    conn.close()

    conn = psycopg2.connect(
        host=host,
        port=port,
        user="pipeline_admin",
        password=password,
        database="postgres"
    )
    cursor = conn.cursor()

    print(f"\nEnforcing prod table schemas")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_performance_metrics (
            product_id VARCHAR(50) PRIMARY KEY,
            total_reviews INT NOT NULL DEFAULT 0,
            avg_urgency_rating NUMERIC(4,2) DEFAULT 0.00,
            negative_ticket_count INT NOT NULL DEFAULT 0,
            positive_ticket_count INT NOT NULL DEFAULT 0,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    print(f"\nRelational DB migrations success")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_migrations()
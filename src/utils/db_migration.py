import os
import json
import boto3
import psycopg2
from botocore.exceptions import ClientError

def get_secret(secret_name):
    """Fetches decrypted JSON secret payload from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"\nFailed to retrieve secret {secret_name}: {e}")
        raise e

def get_admin_credentials():
    if "DB_ENDPOINT" in os.environ and "DB_PASSWORD" in os.environ:
        return os.environ["DB_ENDPOINT"], os.environ["DB_PASSWORD"]

    admin_secret = get_secret("customer-feedback-pipeline-dev-db-credentials")
    return admin_secret["endpoint"], admin_secret["password"]

def run_migrations():
    admin_endpoint, admin_password = get_admin_credentials()
    host, port = admin_endpoint.split(":")

    dashboard_secret = get_secret("customer-feedback-pipeline-dev-dashboard-credentials")
    dashboard_password = dashboard_secret["password"]

    print(f"\nConnecting to relational sink at {host} as admin")

    conn = psycopg2.connect(
        host=host,
        port=port,
        user="pipeline_admin",
        password=admin_password,
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
        password=admin_password,
        database="feedback_analytics"
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

    print(f"\nEnforcing read-only access for dashboard layer")
    
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader';")
    if not cursor.fetchone():
        cursor.execute(f"CREATE ROLE dashboard_reader WITH LOGIN PASSWORD %s;", (dashboard_password,))
    else:
        cursor.execute(f"ALTER ROLE dashboard_reader WITH PASSWORD %s;", (dashboard_password,))

    cursor.execute("GRANT CONNECT ON DATABASE feedback_analytics TO dashboard_reader;")
    cursor.execute("GRANT USAGE ON SCHEMA public TO dashboard_reader;")
    cursor.execute("GRANT SELECT ON TABLE product_performance_metrics TO dashboard_reader;")

    conn.commit()
    print(f"\nRelational DB migrations success")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_migrations()
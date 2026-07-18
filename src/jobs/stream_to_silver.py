import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.llm_triage import call_mock_llm  # noqa: E402


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))

    src_dir = os.path.join(project_root, "src")
    bronze_dir = os.path.join(project_root, "data_lake", "bronze")
    silver_dir = os.path.join(project_root, "data_lake", "silver")
    checkpoint_dir = os.path.join(project_root, "data_lake", "checkpoints", "silver")

    print("\nInitializing local Spark session for bronze\n")
    spark = (
        SparkSession.builder.appName("CustomerFeedbackSilverStreaming")
        .master("local[4]")
        .config("spark.pyspark.driver.acceptConnIfUntrusted", "true")
        # explicit arrow execution optimization
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sys.path.append(src_dir)
    spark.sparkContext.addPyFile(os.path.join(src_dir, "utils", "llm_triage.py"))

    import llm_triage

    call_mock_llm = llm_triage.call_mock_llm

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

    # define struct returned by internal mock LLM schema
    llm_output_schema = StructType(
        [
            StructField("sentiment", StringType(), True),
            StructField("urgency_score", IntegerType(), True),
            StructField("summary", StringType(), True),
        ]
    )

    # Declare the vectorized UDF
    @udf(returnType=StringType())
    def serialize_llm_triage_udf(reviews: pd.Series) -> pd.Series:
        return reviews.apply(call_mock_llm)

    print(f"\nMonitoring Bronze parquet storage tier: {bronze_dir}")

    # read from Parquet source stream (utilizes directory schemas)
    bronze_stream_df = (
        spark.readStream.format("parquet").schema(bronze_schema).load(bronze_dir)
    )

    silver_enriched_df = bronze_stream_df.withColumn(
        "raw_llm_json", serialize_llm_triage_udf(col("review_text"))
    )

    # unpack the raw text JSON coluns string into typed struct elements
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

    print(f"\nDirecting enriched stream output to Silver tier: {silver_dir}")

    query = (
        silver_final_df.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start(silver_dir)
    )

    try:
        query.awaitTermination()
        print("\nSilver micro-batch execution completed successfully.")
        spark.stop()
        sys.exit(0)
    except Exception as e:
        print(f"\nStream tracking execution failed: {str(e)}")
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

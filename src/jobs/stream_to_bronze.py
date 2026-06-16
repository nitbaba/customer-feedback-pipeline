import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp

def main():
    # Find project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))

    landing_dir = os.path.join(project_root, "data_lake", "landing")
    checkpoint_dir = os.path.join(project_root, "data_lake", "checkpoints", "bronze")

    print("Initializing local Spark session")

    spark = (SparkSession.builder
             .appName("CustomerFeedbackBronzeStraming")
             .master("local[4]")
             .getOrCreate())

    #reduce logging
    spark.sparkContext.setLogLevel("WARN")

    print("Defining expected incoming Data scheme")

    schema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print(f"Monitor Landing Zone: {landing_dir}")
    #readStream, tells Spark to treat dir as live data source
    raw_stream_df = (spark.readStream
                     .schema(schema)
                     .option("multiLine", "true")
                     .json(landing_dir))

    #Prod pattern to note: adding timestamp to Bronze layer df
    bronze_enriched_df = raw_stream_df.withColumn("ingested_at", current_timestamp())

    print("Streaming engine started. Waiting for data batches")
    print(f"send sigkill(ctrl+c) to stop.\n")

    #writeStream, tells destination. For local test, goes to terminal
    query = (bronze_enriched_df.writeStream
             .format("console")
             .outputMode("append")
             #checkpoint for data projress(file offets)
             .option("checkpointLocation", checkpoint_dir)
             .start())

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n Shutdown received. Stopping now.")

        #stops streaming thread from reading files
        query.stop()

        #stop spark engine cluster that was spun up
        spark.stop()

        print("Session stopped safely.")
        sys.exit(0)

if __name__ == "__main__":
    main()


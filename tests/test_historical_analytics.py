import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import avg, count

@pytest.fixture(scope = "session")
def spark_session():
    """Local spark engine for test"""
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("PipelineUnitTests")
             .getOrCreate())
    yield spark
    spark.stop()

def test_gold_aggregation_logic(spark_session):
    #Mimick incoming schema like silver tier output
    silver_schema = StructType([
        StructField("ticket_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("urgency_score", DoubleType(), True)
    ])

    mock_data = [
        ("TCK-101", "PROD-A", 4.5),
        ("TCK-102", "PROD-A", 1.5),
        ("TCK-103", "PROD-B", 3.0),
        ("TCK-104", "PROD-A", 3.0)
    ]

    silver_df = spark_session.createDataFrame(mock_data, schema=silver_schema)

    #Mimick prod aggregate transformation
    gold_metrics_df = (silver_df
                       .groupBy("product_id")
                       .agg(
                            count("ticket_id").alias("total_reviews"),
                            avg("urgency_score").alias("avg_urgency")
                       ))

    results = {row["product_id"]: (row["total_reviews"], row["avg_urgency"])
               for row in gold_metrics_df.collect()}

    assert "PROD-A" in results
    assert "PROD-B" in results

    assert results["PROD-A"][0] == 3
    assert pytest.approx(results["PROD-A"][1]) == 3.0

    assert results["PROD-B"][0] == 1
    assert pytest.approx(results["PROD-A"][1]) == 3.0

"""
Wikimedia Kafka → PySpark Structured Streaming
------------------------------------------------
Reads from the wikimedia-recentchange Kafka topic,
parses JSON, enriches, and prints to console for now.
BigQuery sink comes after we verify this works.
"""
import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, IntegerType
)

logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:9092"
KAFKA_TOPIC = "wikimedia-recentchange"

# Schema for the Wikimedia recent change event
SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("server_name", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("dt", StringType(), True),
    ]), True),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("wikimedia-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading from Kafka...")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON
    parsed = (
        raw
        .select(
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("data")
        )
        .select("offset", "kafka_timestamp", "data.*")
    )

    # Enrich
    enriched = (
        parsed
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withColumn("is_bot", F.col("bot").cast(BooleanType()))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "offset",
            "id",
            "type",
            "title",
            "namespace",
            "user",
            "is_bot",
            "wiki",
            "server_name",
            "comment",
            "event_time",
            "processed_at",
        )
    )

    # Print to console for now
    query = (
        enriched.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 5)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
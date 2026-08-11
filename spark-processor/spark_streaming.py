#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   ⚡  FoodStream Analytics — PySpark Stream Processor    ║
║                                                          ║
║  Reads raw orders from Kafka, enriches them with         ║
║  computed fields, and writes processed events to a       ║
║  second Kafka topic (consumed by Apache Pinot).          ║
╚══════════════════════════════════════════════════════════╝
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, from_unixtime,
    hour, when, lit, current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, FloatType, BooleanType,
)

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("SparkProcessor")

# ─── Config ──────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "localhost:29092")
INPUT_TOPIC    = os.getenv("INPUT_TOPIC",    "raw-food-orders")
OUTPUT_TOPIC   = os.getenv("OUTPUT_TOPIC",   "processed-food-orders")
CHECKPOINT_DIR = "/tmp/spark-checkpoint"

# ─── Raw Order Schema ─────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("order_id",           StringType(),  True),
    StructField("timestamp",          LongType(),    True),
    StructField("customer_name",      StringType(),  True),
    StructField("customer_phone",     StringType(),  True),
    StructField("city",               StringType(),  True),
    StructField("area",               StringType(),  True),
    StructField("restaurant_name",    StringType(),  True),
    StructField("cuisine",            StringType(),  True),
    StructField("items",              StringType(),  True),
    StructField("quantity",           IntegerType(), True),
    StructField("price",              FloatType(),   True),
    StructField("status",             StringType(),  True),
    StructField("payment_method",     StringType(),  True),
    StructField("delivery_time_mins", IntegerType(), True),
    StructField("rating",             FloatType(),   True),
    StructField("discount_applied",   BooleanType(), True),
    StructField("is_first_order",     BooleanType(), True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FoodOrderStreamProcessor")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def process_stream(spark: SparkSession):
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  ⚡  PySpark Structured Streaming")
    print(f"  Input  → Kafka: {INPUT_TOPIC}")
    print(f"  Output → Kafka: {OUTPUT_TOPIC}")
    print("=" * 60)

    # ── 1. Read from Kafka ──────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload ───────────────────────────────
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
        .select("d.*")
    )

    # ── 3. Enrich with computed fields ──────────────────────
    enriched = (
        parsed
        # Hour of the day (0-23) derived from epoch ms timestamp
        .withColumn(
            "hour_of_day",
            hour(from_unixtime(col("timestamp") / 1000))
        )
        # Peak hours: lunch (12-14) and dinner (19-22)
        .withColumn(
            "is_peak_hour",
            when(
                col("hour_of_day").between(12, 14) |
                col("hour_of_day").between(19, 22),
                lit(1)
            ).otherwise(lit(0))
        )
        # Bucket orders by price
        .withColumn(
            "order_value_category",
            when(col("price") < 200, lit("Budget"))
            .when(col("price") < 600, lit("Mid-Range"))
            .otherwise(lit("Premium"))
        )
        # Revenue tier per item (price / quantity)
        .withColumn(
            "price_per_item",
            (col("price") / col("quantity")).cast(FloatType())
        )
        # Flag high-value orders (above ₹1000)
        .withColumn(
            "is_high_value",
            when(col("price") >= 1000, lit(1)).otherwise(lit(0))
        )
        # Discount flag as int for Pinot compatibility
        .withColumn(
            "discount_applied",
            when(col("discount_applied") == True, lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "is_first_order",
            when(col("is_first_order") == True, lit(1)).otherwise(lit(0))
        )
    )

    # ── 4. Serialize to JSON for output Kafka topic ──────────
    output = enriched.select(
        col("order_id").alias("key"),
        to_json(
            struct([enriched[c] for c in enriched.columns])
        ).alias("value")
    )

    # ── 5. Write to processed-food-orders Kafka topic ────────
    query = (
        output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    print(f"  ✅ Stream query started — ID: {query.id}")
    print(f"  📡 Consuming from '{INPUT_TOPIC}' → Publishing to '{OUTPUT_TOPIC}'")
    print("  🔄 Processing in real-time... (Ctrl+C to stop)")

    query.awaitTermination()


def main():
    spark = build_spark_session()
    try:
        process_stream(spark)
    except KeyboardInterrupt:
        print("\n🛑 Spark processor stopped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

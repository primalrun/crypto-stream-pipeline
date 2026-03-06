"""
Spark Structured Streaming job — Kafka → Snowflake.

Runs two concurrent streaming queries:
  1. Raw trades   — trigger every 10s  → CRYPTO.RAW.trades
  2. 1-min OHLCV  — trigger every 60s  → CRYPTO.RAW.prices_1min

Start via:
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client \
        /opt/spark-jobs/stream_prices.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

SF_OPTIONS = {
    "sfURL": os.environ["SNOWFLAKE_ACCOUNT"] + ".snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": "CRYPTO",
    "sfSchema": "RAW",
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
}

TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("trade_id", LongType()),
        StructField("timestamp_ms", LongType()),
        StructField("is_buyer_maker", BooleanType()),
    ]
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_batch(table: str):
    """Return a foreachBatch writer function that appends to a Snowflake table."""

    def _write(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df.write.format("snowflake")
            .options(**SF_OPTIONS)
            .option("dbtable", table)
            .mode("append")
            .save()
        )

    return _write


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    spark = (
        SparkSession.builder.appName("CryptoStreamPipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read raw bytes from Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON payload; derive trade_time and kafka_timestamp as timestamps
    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        ).select(
            F.col("d.symbol").alias("symbol"),
            F.col("d.price").alias("price"),
            F.col("d.quantity").alias("quantity"),
            F.col("d.trade_id").alias("trade_id"),
            F.col("d.timestamp_ms").alias("timestamp_ms"),
            F.col("d.is_buyer_maker").alias("is_buyer_maker"),
            (F.col("d.timestamp_ms") / 1000).cast("timestamp").alias("trade_time"),
            F.col("kafka_ts").cast("timestamp").alias("kafka_timestamp"),
        )
    )

    # ------------------------------------------------------------------
    # Query 1: raw trades → CRYPTO.RAW.trades  (trigger every 10s)
    # ------------------------------------------------------------------
    trades_query = (
        parsed.writeStream.trigger(processingTime="10 seconds")
        .foreachBatch(write_batch("trades"))
        .option("checkpointLocation", "/tmp/checkpoints/trades")
        .start()
    )

    # ------------------------------------------------------------------
    # Query 2: 1-minute OHLCV → CRYPTO.RAW.prices_1min  (trigger every 60s)
    #
    # withWatermark(2 min) lets Spark discard late state and emit finalized
    # windows in append mode — each window is written exactly once.
    # ------------------------------------------------------------------
    ohlcv = (
        parsed.withWatermark("trade_time", "2 minutes")
        .groupBy(F.window("trade_time", "1 minute"), "symbol")
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("trade_count"),
        )
    )

    prices_query = (
        ohlcv.writeStream.trigger(processingTime="60 seconds")
        .outputMode("append")
        .foreachBatch(write_batch("prices_1min"))
        .option("checkpointLocation", "/tmp/checkpoints/prices")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

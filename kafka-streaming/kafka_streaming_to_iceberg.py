"""
kafka_streaming_to_iceberg.py
============================
Spark 4.1 Structured Streaming：消费 MSK Kafka topic `agent.spans`，
解析 JSON 消息，将 attributes 字段转成 VARIANT 类型，
写入 S3 Tables Iceberg v3 表 s3tablesbucket.agentdb.agent_spans。

关键设计：
  - get_json_object 处理带点字段名（service.name / gen_ai.conversation.id / user.id）
  - parse_json() 将 attributes JSON 字符串转为 VARIANT（触发 Shredding）
  - foreachBatch + writeTo(...).append() 保证 Iceberg VARIANT 类型写入兼容
  - days(start_time) 分区
  - processingTime trigger，60 秒一批
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    get_json_object,
    to_timestamp,
    parse_json,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("KafkaStreamingToIceberg")

# --------------------------------------------------------------------------
# 目标表完整名称
# --------------------------------------------------------------------------
CATALOG = "s3tablesbucket"
TABLE_FULL_NAME = "s3tablesbucket.tracelog.agent_spans"

# --------------------------------------------------------------------------
# Iceberg 建表 DDL
# --------------------------------------------------------------------------
CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS s3tablesbucket.tracelog.agent_spans (
    trace_id        STRING,
    span_id         STRING,
    parent_span_id  STRING,
    name            STRING,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    duration_ms     BIGINT,
    status_code     STRING,
    attempt         INT,
    service_name    STRING,
    conversation_id STRING,
    user_id         STRING,
    attributes      VARIANT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd'
)
"""


# --------------------------------------------------------------------------
# 解析命令行参数
# --------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka Structured Streaming → S3 Tables Iceberg VARIANT+Shredding"
    )
    parser.add_argument(
        "--bootstrap",
        default=(
            "boot-929.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
            "boot-hnb.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
            "boot-fbo.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092"
        ),
        help="Kafka bootstrap servers（逗号分隔）",
    )
    parser.add_argument(
        "--topic",
        default="agent.spans",
        help="Kafka topic 名称（默认 agent.spans）",
    )
    parser.add_argument(
        "--checkpoint",
        default="s3a://adap-prototype-812046859005/variant-shredding-test/checkpoints/kafka-streaming-v2/",
        help="Streaming checkpoint 路径（S3 路径）",
    )
    parser.add_argument(
        "--trigger-interval",
        default="60 seconds",
        help="Trigger 间隔，例如 '60 seconds'（默认 60 seconds）",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        help="Kafka 起始偏移量：latest（默认）或 earliest（首次回溯历史消息）",
    )
    parser.add_argument(
        "--namespace",
        default="tracelog",
        help="Iceberg namespace（默认 agentdb）",
    )
    return parser.parse_args()


# --------------------------------------------------------------------------
# 确保 namespace 和表存在
# --------------------------------------------------------------------------
def ensure_table(spark: SparkSession, namespace: str) -> None:
    """
    若 namespace 不存在则创建；验证目标表存在。
    注意：表需通过 AWS CLI 或 Java 端预先创建（S3 Tables REST API 对 VARIANT DDL 有限制）。
    """
    # 检查并创建 namespace
    namespaces = [
        row[0]
        for row in spark.sql(
            f"SHOW NAMESPACES IN {CATALOG}"
        ).collect()
    ]
    if namespace not in namespaces:
        logger.info("Namespace %s 不存在，正在创建……", namespace)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
        logger.info("Namespace %s 创建完成", namespace)
    else:
        logger.info("Namespace %s 已存在", namespace)

    # 检查表是否已存在
    try:
        tables = [
            row[1]
            for row in spark.sql(
                f"SHOW TABLES IN {CATALOG}.{namespace}"
            ).collect()
        ]
        if "agent_spans" in tables:
            logger.info("目标表 %s 已存在，跳过建表", TABLE_FULL_NAME)
            return
    except Exception as e:
        logger.warning("检查表列表失败: %s", e)

    # 尝试建表
    logger.info("执行建表 DDL……")
    try:
        spark.sql(CREATE_TABLE_DDL)
        logger.info("目标表 %s 创建成功", TABLE_FULL_NAME)
    except Exception as e:
        logger.warning("DDL 建表失败（%s），尝试通过 writeTo 自动建表，将在首个 batch 时创建", e)
        # 不抛异常，让 streaming 启动后通过 writeTo 的 create 语义自动建表


# --------------------------------------------------------------------------
# 解析 Kafka value（JSON 字符串）为目标 DataFrame
# --------------------------------------------------------------------------
def parse_kafka_value(raw_df):
    """
    raw_df 包含 Kafka 原始列（value 为 binary）。
    返回与目标表 schema 对齐的 DataFrame，attributes 为 VARIANT。

    特别处理：
      - JSON 中带点的字段名用 get_json_object 的转义语法 \\. 提取
      - attributes 先提取为 JSON 字符串，再用 parse_json() 转 VARIANT
      - start_time / end_time 用 to_timestamp 解析 ISO 8601
    """
    df = raw_df.selectExpr("CAST(value AS STRING) AS value_str")

    # 普通字段
    df = df.select(
        get_json_object(col("value_str"), "$.trace_id").alias("trace_id"),
        get_json_object(col("value_str"), "$.span_id").alias("span_id"),
        get_json_object(col("value_str"), "$.parent_span_id").alias("parent_span_id"),
        get_json_object(col("value_str"), "$.name").alias("name"),
        to_timestamp(
            get_json_object(col("value_str"), "$.start_time")
        ).alias("start_time"),
        to_timestamp(
            get_json_object(col("value_str"), "$.end_time")
        ).alias("end_time"),
        get_json_object(col("value_str"), "$.duration_ms").cast("BIGINT").alias("duration_ms"),
        get_json_object(col("value_str"), "$.status_code").alias("status_code"),
        get_json_object(col("value_str"), "$.attempt").cast("INT").alias("attempt"),
        # 带点字段名：用 \\. 转义，映射为无点列名
        get_json_object(col("value_str"), "$.service\\.name").alias("service_name"),
        get_json_object(col("value_str"), "$.gen_ai\\.conversation\\.id").alias("conversation_id"),
        get_json_object(col("value_str"), "$.user\\.id").alias("user_id"),
        # attributes：先提取 JSON 字符串，再转 VARIANT（触发 Shredding）
        parse_json(
            get_json_object(col("value_str"), "$.attributes")
        ).alias("attributes"),
    )
    return df


# --------------------------------------------------------------------------
# foreachBatch 写入 Iceberg（兼容 VARIANT 类型）
# --------------------------------------------------------------------------
def write_batch_to_iceberg(batch_df, batch_id: int) -> None:
    """
    用 writeTo(...).append() 写入 Iceberg。
    首次写入时若表为空（通过 AWS CLI 创建的空表），使用 createOrReplace 初始化 schema。
    """
    count = batch_df.count()
    if count == 0:
        logger.info("Batch %d：空批次，跳过写入", batch_id)
        return

    logger.info("Batch %d：写入 %d 条记录到 %s", batch_id, count, TABLE_FULL_NAME)
    try:
        (
            batch_df.writeTo(TABLE_FULL_NAME)
            .append()
        )
    except Exception as e:
        err_msg = str(e)
        if "INCOMPATIBLE_DATA_FOR_TABLE" in err_msg or "Cannot write" in err_msg or "schema" in err_msg.lower():
            # 表可能是空表（无 schema），尝试用 create 初始化
            logger.warning("Batch %d：append 失败，尝试 create 初始化表 schema: %s", batch_id, e)
            (
                batch_df.writeTo(TABLE_FULL_NAME)
                .tableProperty("format-version", "3")
                .tableProperty("write.parquet.compression-codec", "zstd")
                .create()
            )
        else:
            raise
    logger.info("Batch %d：写入完成", batch_id)


# --------------------------------------------------------------------------
# 主程序
# --------------------------------------------------------------------------
def main():
    args = parse_args()

    logger.info("初始化 SparkSession……")
    spark = (
        SparkSession.builder
        .appName("KafkaStreamingToIceberg-AgentSpans")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("参数摘要：")
    logger.info("  bootstrap       = %s", args.bootstrap)
    logger.info("  topic           = %s", args.topic)
    logger.info("  checkpoint      = %s", args.checkpoint)
    logger.info("  trigger         = %s", args.trigger_interval)
    logger.info("  startingOffsets = %s", args.starting_offsets)
    logger.info("  namespace       = %s", args.namespace)

    # 1. 确保目标 namespace 和表就绪
    ensure_table(spark, args.namespace)

    # 2. 从 Kafka 读取 Streaming DataFrame
    logger.info("连接 Kafka，订阅 topic: %s", args.topic)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        # 防止单批次拉取过多消息导致 OOM
        .option("maxOffsetsPerTrigger", 500_000)
        # Kafka 连接安全选项（MSK 使用 PLAINTEXT，若需要 TLS 可按需修改）
        .option("kafka.security.protocol", "PLAINTEXT")
        .load()
    )

    # 3. 解析 JSON → 目标 schema（含 VARIANT）
    parsed_stream = parse_kafka_value(raw_stream)

    # 4. 启动 Streaming Query，foreachBatch 写 Iceberg
    logger.info("启动 Streaming Query，trigger='%s'", args.trigger_interval)
    query = (
        parsed_stream.writeStream
        .foreachBatch(write_batch_to_iceberg)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    logger.info("Streaming Query 已启动，等待终止（Ctrl+C 或 Pod 停止）……")
    query.awaitTermination()


if __name__ == "__main__":
    main()

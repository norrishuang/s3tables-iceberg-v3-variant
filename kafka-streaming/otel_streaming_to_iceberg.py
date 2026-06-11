"""
otel_streaming_to_iceberg.py
============================
Spark 4.1 Structured Streaming：消费 MSK Kafka topic `agent.spans.otlp`，
解析 OpenTelemetry (OTel) 标准格式 JSON，写入 S3 Tables Iceberg v3 表。

OTel 格式特点：
  - 三层嵌套：resourceSpans > scopeSpans > spans
  - 时间戳为纳秒级 Unix int（startTimeUnixNano / endTimeUnixNano）
  - attributes 为 key-value 数组格式，需转为扁平 map
  - events 为数组（gen_ai.user.message / gen_ai.assistant.message）

目标表 schema：
  - 结构化字段：trace_id, span_id, start_time 等（高频查询，支持分区下推）
  - VARIANT + Shredding：attributes（扁平 map）、events、resource_attributes
  - 分区：days(start_time)
"""

import argparse
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, from_json, lit, to_json, parse_json, udf,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, LongType, IntegerType, MapType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("OtelStreamingToIceberg")

# --------------------------------------------------------------------------
# 目标表
# --------------------------------------------------------------------------
CATALOG = "s3tablesbucket"
NAMESPACE = "tracelog"
TABLE_NAME = "otel_spans"
TABLE_FULL_NAME = f"{CATALOG}.{NAMESPACE}.{TABLE_NAME}"

CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_FULL_NAME} (
    trace_id            STRING,
    span_id             STRING,
    parent_span_id      STRING,
    name                STRING,
    kind                INT,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_ms         BIGINT,
    status_code         INT,
    service_name        STRING,
    service_version     STRING,
    deployment_env      STRING,
    attributes          VARIANT,
    events              VARIANT,
    resource_attributes VARIANT
)
USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd'
)
"""

# --------------------------------------------------------------------------
# OTel JSON Schema（用于 from_json 解析）
# --------------------------------------------------------------------------
OTEL_ATTR_SCHEMA = ArrayType(StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),  # 先当 string，后面用 UDF 提取
]))

OTEL_EVENT_SCHEMA = ArrayType(StructType([
    StructField("timeUnixNano", StringType()),
    StructField("name", StringType()),
    StructField("attributes", StringType()),
]))

SPAN_SCHEMA = StructType([
    StructField("traceId", StringType()),
    StructField("spanId", StringType()),
    StructField("parentSpanId", StringType()),
    StructField("name", StringType()),
    StructField("kind", IntegerType()),
    StructField("startTimeUnixNano", StringType()),
    StructField("endTimeUnixNano", StringType()),
    StructField("status", StructType([StructField("code", IntegerType())])),
    StructField("attributes", StringType()),  # raw JSON string
    StructField("events", StringType()),      # raw JSON string
])

SCOPE_SPANS_SCHEMA = StructType([
    StructField("scope", StructType([
        StructField("name", StringType()),
        StructField("version", StringType()),
    ])),
    StructField("spans", ArrayType(SPAN_SCHEMA)),
])

RESOURCE_SPANS_SCHEMA = ArrayType(StructType([
    StructField("resource", StructType([
        StructField("attributes", StringType()),  # raw JSON string
    ])),
    StructField("scopeSpans", ArrayType(SCOPE_SPANS_SCHEMA)),
]))

OTEL_ROOT_SCHEMA = StructType([
    StructField("resourceSpans", RESOURCE_SPANS_SCHEMA),
])


# --------------------------------------------------------------------------
# UDF：将 OTel attributes 数组转为扁平 map JSON
# --------------------------------------------------------------------------
@udf(StringType())
def flatten_otel_attributes(attrs_json: str) -> str:
    """
    将 [{"key":"k", "value":{"stringValue":"v"}},...] 转为 {"k": "v", ...}
    value 类型自动提取：stringValue, intValue, doubleValue, boolValue, arrayValue
    """
    if not attrs_json:
        return "{}"
    try:
        attrs = json.loads(attrs_json)
        if not isinstance(attrs, list):
            return attrs_json  # 已经是 map 格式
        result = {}
        for attr in attrs:
            key = attr.get("key", "")
            value_obj = attr.get("value", {})
            if isinstance(value_obj, dict):
                # 提取具体类型的值
                if "stringValue" in value_obj:
                    result[key] = value_obj["stringValue"]
                elif "intValue" in value_obj:
                    result[key] = int(value_obj["intValue"])
                elif "doubleValue" in value_obj:
                    result[key] = value_obj["doubleValue"]
                elif "boolValue" in value_obj:
                    result[key] = value_obj["boolValue"]
                elif "arrayValue" in value_obj:
                    # 提取 arrayValue.values
                    values = value_obj["arrayValue"].get("values", [])
                    result[key] = [
                        v.get("stringValue", v.get("intValue", v.get("doubleValue", str(v))))
                        for v in values
                    ]
                else:
                    result[key] = value_obj
            else:
                result[key] = value_obj
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return attrs_json


@udf(StringType())
def extract_otel_attr_value(attrs_json: str, key: str) -> str:
    """从 OTel attributes 数组中提取指定 key 的值"""
    if not attrs_json:
        return None
    try:
        attrs = json.loads(attrs_json)
        if not isinstance(attrs, list):
            return None
        for attr in attrs:
            if attr.get("key") == key:
                value_obj = attr.get("value", {})
                if isinstance(value_obj, dict):
                    return (value_obj.get("stringValue")
                            or str(value_obj.get("intValue", ""))
                            or str(value_obj.get("doubleValue", ""))
                            or None)
                return str(value_obj)
        return None
    except Exception:
        return None


@udf(StringType())
def flatten_otel_events(events_json: str) -> str:
    """
    将 events 数组中的 attributes 也转为扁平 map 格式
    """
    if not events_json:
        return "[]"
    try:
        events = json.loads(events_json)
        if not isinstance(events, list):
            return events_json
        result = []
        for event in events:
            flat_event = {
                "timeUnixNano": event.get("timeUnixNano"),
                "name": event.get("name"),
            }
            # 扁平化 event attributes
            evt_attrs = event.get("attributes", [])
            if isinstance(evt_attrs, list):
                flat_attrs = {}
                for attr in evt_attrs:
                    k = attr.get("key", "")
                    v = attr.get("value", {})
                    if isinstance(v, dict):
                        flat_attrs[k] = (v.get("stringValue")
                                         or v.get("intValue")
                                         or v.get("doubleValue")
                                         or v.get("boolValue")
                                         or v)
                    else:
                        flat_attrs[k] = v
                flat_event["attributes"] = flat_attrs
            else:
                flat_event["attributes"] = evt_attrs
            result.append(flat_event)
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return events_json


# --------------------------------------------------------------------------
# 命令行参数
# --------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="OTel Kafka Streaming → S3 Tables Iceberg")
    parser.add_argument("--bootstrap", default=(
        "boot-929.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
        "boot-hnb.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
        "boot-fbo.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092"
    ))
    parser.add_argument("--topic", default="agent.spans.otlp")
    parser.add_argument("--checkpoint", default="s3a://adap-prototype-812046859005/variant-shredding-test/checkpoints/otel-streaming/")
    parser.add_argument("--trigger-interval", default="120 seconds")
    parser.add_argument("--starting-offsets", default="latest")
    return parser.parse_args()


# --------------------------------------------------------------------------
# 确保表存在
# --------------------------------------------------------------------------
def ensure_table(spark: SparkSession) -> None:
    namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
    if NAMESPACE not in namespaces:
        logger.info("创建 namespace %s", NAMESPACE)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    tables = [row[1] for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").collect()]
    if TABLE_NAME in tables:
        logger.info("目标表 %s 已存在", TABLE_FULL_NAME)
        return

    logger.info("执行建表 DDL……")
    try:
        spark.sql(CREATE_TABLE_DDL)
        logger.info("目标表 %s 创建成功", TABLE_FULL_NAME)
    except Exception as e:
        logger.warning("DDL 建表失败: %s，将在首批写入时通过 writeTo.create() 建表", e)


# --------------------------------------------------------------------------
# 解析 OTel JSON → 目标 schema
# --------------------------------------------------------------------------
def parse_otel_message(raw_df):
    """
    将 Kafka value（OTel JSON）解析为目标表 schema 的 DataFrame。
    一条 Kafka 消息可能包含多个 resourceSpans > scopeSpans > spans，需要 explode。
    """
    # 1. 解析 JSON 结构
    df = raw_df.selectExpr("CAST(value AS STRING) AS raw_json")

    parsed = df.select(
        from_json(col("raw_json"), OTEL_ROOT_SCHEMA).alias("otel")
    ).select(
        explode(col("otel.resourceSpans")).alias("rs")
    )

    # 2. 展开 scopeSpans → spans
    exploded = parsed.select(
        col("rs.resource.attributes").alias("resource_attrs_raw"),
        explode(col("rs.scopeSpans")).alias("ss"),
    ).select(
        col("resource_attrs_raw"),
        explode(col("ss.spans")).alias("span"),
    )

    # 3. 提取结构化字段 + VARIANT 字段
    result = exploded.select(
        col("span.traceId").alias("trace_id"),
        col("span.spanId").alias("span_id"),
        col("span.parentSpanId").alias("parent_span_id"),
        col("span.name").alias("name"),
        col("span.kind").alias("kind"),
        # 纳秒 → TIMESTAMP
        (col("span.startTimeUnixNano").cast("long") / 1_000_000_000).cast("timestamp").alias("start_time"),
        (col("span.endTimeUnixNano").cast("long") / 1_000_000_000).cast("timestamp").alias("end_time"),
        # duration_ms
        ((col("span.endTimeUnixNano").cast("long") - col("span.startTimeUnixNano").cast("long")) / 1_000_000).cast("bigint").alias("duration_ms"),
        col("span.status.code").alias("status_code"),
        # 从 resource attributes 提取 service 信息
        extract_otel_attr_value(col("resource_attrs_raw"), lit("service.name")).alias("service_name"),
        extract_otel_attr_value(col("resource_attrs_raw"), lit("service.version")).alias("service_version"),
        extract_otel_attr_value(col("resource_attrs_raw"), lit("deployment.environment")).alias("deployment_env"),
        # VARIANT 字段：扁平化后转 VARIANT
        parse_json(flatten_otel_attributes(col("span.attributes"))).alias("attributes"),
        parse_json(flatten_otel_events(col("span.events"))).alias("events"),
        parse_json(flatten_otel_attributes(col("resource_attrs_raw"))).alias("resource_attributes"),
    )
    return result


# --------------------------------------------------------------------------
# foreachBatch 写入
# --------------------------------------------------------------------------
def write_batch(batch_df, batch_id: int) -> None:
    count = batch_df.count()
    if count == 0:
        logger.info("Batch %d：空批次，跳过", batch_id)
        return

    logger.info("Batch %d：写入 %d 条到 %s", batch_id, count, TABLE_FULL_NAME)
    try:
        batch_df.writeTo(TABLE_FULL_NAME).append()
    except Exception as e:
        err_msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in err_msg:
            logger.warning("Batch %d：表不存在，尝试 create", batch_id)
            (batch_df.writeTo(TABLE_FULL_NAME)
             .tableProperty("format-version", "3")
             .tableProperty("write.parquet.compression-codec", "zstd")
             .partitionedBy(col("start_time"))
             .create())
        else:
            raise
    logger.info("Batch %d：写入完成", batch_id)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    args = parse_args()

    spark = SparkSession.builder.appName("OtelStreamingToIceberg").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("参数: topic=%s, trigger=%s, checkpoint=%s",
                args.topic, args.trigger_interval, args.checkpoint)

    # 注册 UDF
    spark.udf.register("flatten_otel_attributes", flatten_otel_attributes)
    spark.udf.register("extract_otel_attr_value", extract_otel_attr_value)
    spark.udf.register("flatten_otel_events", flatten_otel_events)

    # 建表
    ensure_table(spark)

    # Kafka source
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", 500_000)
        .option("kafka.security.protocol", "PLAINTEXT")
        .load()
    )

    parsed_stream = parse_otel_message(raw_stream)

    query = (
        parsed_stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    logger.info("Streaming Query 已启动")
    query.awaitTermination()


if __name__ == "__main__":
    main()

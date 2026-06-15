"""
otel_streaming_json.py
======================
与 otel_streaming_to_iceberg.py 对比版本：
  - attributes / events / resource_attributes 存为 STRING（JSON 字符串）
  - 不使用 VARIANT 类型
  - 目标表：s3tablesbucket.tracelog.otel_spans_json
  - 用于对比 VARIANT+Shredding vs STRING 的查询性能差异
"""

import argparse
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, from_json, lit, to_json, udf,
    get_json_object, posexplode, format_string,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, LongType,
    IntegerType, DoubleType, BooleanType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("OtelStreamingJson")

CATALOG = "s3tablesbucket"
NAMESPACE = "tracelog"
TABLE_NAME = "otel_spans_json"
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
    attributes          STRING,
    events              STRING,
    resource_attributes STRING
)
USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd'
)
"""

# --------------------------------------------------------------------------
# OTel JSON Schema（与 VARIANT 版相同）
# --------------------------------------------------------------------------
OTEL_ATTR_VALUE_SCHEMA = StructType([
    StructField("stringValue", StringType()),
    StructField("intValue", StringType()),
    StructField("doubleValue", DoubleType()),
    StructField("boolValue", BooleanType()),
    StructField("arrayValue", StructType([
        StructField("values", ArrayType(StructType([
            StructField("stringValue", StringType()),
            StructField("intValue", StringType()),
            StructField("doubleValue", DoubleType()),
        ])))
    ])),
])

OTEL_KV_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("key", StringType()),
    StructField("value", OTEL_ATTR_VALUE_SCHEMA),
]))

OTEL_EVENT_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("timeUnixNano", StringType()),
    StructField("name", StringType()),
    StructField("attributes", OTEL_KV_ARRAY_SCHEMA),
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
    StructField("attributes", OTEL_KV_ARRAY_SCHEMA),
    StructField("events", OTEL_EVENT_ARRAY_SCHEMA),
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
        StructField("attributes", OTEL_KV_ARRAY_SCHEMA),
    ])),
    StructField("scopeSpans", ArrayType(SCOPE_SPANS_SCHEMA)),
]))

OTEL_ROOT_SCHEMA = StructType([
    StructField("resourceSpans", RESOURCE_SPANS_SCHEMA),
])


# --------------------------------------------------------------------------
# UDF：扁平化（与 VARIANT 版相同逻辑）
# --------------------------------------------------------------------------
@udf(StringType())
def flatten_otel_attributes(attrs_json: str) -> str:
    if not attrs_json:
        return "{}"
    try:
        attrs = json.loads(attrs_json)
        if not isinstance(attrs, list):
            return attrs_json
        result = {}
        for attr in attrs:
            key = attr.get("key", "")
            value_obj = attr.get("value", {})
            if isinstance(value_obj, dict):
                if "stringValue" in value_obj:
                    result[key] = value_obj["stringValue"]
                elif "intValue" in value_obj:
                    result[key] = int(value_obj["intValue"])
                elif "doubleValue" in value_obj:
                    result[key] = value_obj["doubleValue"]
                elif "boolValue" in value_obj:
                    result[key] = value_obj["boolValue"]
                elif "arrayValue" in value_obj:
                    values = value_obj["arrayValue"].get("values", [])
                    result[key] = [v.get("stringValue", v.get("intValue", v.get("doubleValue", str(v)))) for v in values]
                else:
                    result[key] = value_obj
            else:
                result[key] = value_obj
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return attrs_json


@udf(StringType())
def flatten_otel_events(events_json: str) -> str:
    if not events_json:
        return "[]"
    try:
        events = json.loads(events_json)
        if not isinstance(events, list):
            return events_json
        result = []
        for event in events:
            flat_event = {"timeUnixNano": event.get("timeUnixNano"), "name": event.get("name")}
            evt_attrs = event.get("attributes", [])
            if isinstance(evt_attrs, list):
                flat_attrs = {}
                for attr in evt_attrs:
                    k = attr.get("key", "")
                    v = attr.get("value", {})
                    if isinstance(v, dict):
                        flat_attrs[k] = v.get("stringValue") or v.get("intValue") or v.get("doubleValue") or v.get("boolValue") or v
                    else:
                        flat_attrs[k] = v
                flat_event["attributes"] = flat_attrs
            else:
                flat_event["attributes"] = evt_attrs
            result.append(flat_event)
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return events_json


@udf(StringType())
def extract_otel_attr_value(attrs_json: str, key: str) -> str:
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
                    return value_obj.get("stringValue") or str(value_obj.get("intValue", "")) or None
                return str(value_obj)
        return None
    except Exception:
        return None


# --------------------------------------------------------------------------
# 解析 OTel JSON（与 VARIANT 版相同逻辑，最终存 STRING 而非 VARIANT）
# --------------------------------------------------------------------------
def parse_otel_message(raw_df):
    df = raw_df.selectExpr("CAST(value AS STRING) AS raw_json")

    parsed = df.select(
        col("raw_json"),
        from_json(col("raw_json"), OTEL_ROOT_SCHEMA).alias("otel")
    ).select(
        col("raw_json"),
        posexplode(col("otel.resourceSpans")).alias("rs_idx", "rs")
    )

    exploded = parsed.select(
        col("raw_json"),
        col("rs_idx"),
        col("rs.resource.attributes").alias("resource_attrs_struct"),
        posexplode(col("rs.scopeSpans")).alias("ss_idx", "ss"),
    ).select(
        col("raw_json"),
        col("rs_idx"),
        col("ss_idx"),
        col("resource_attrs_struct"),
        posexplode(col("ss.spans")).alias("span_idx", "span"),
    )

    result = exploded.selectExpr(
        "span.traceId AS trace_id",
        "span.spanId AS span_id",
        "span.parentSpanId AS parent_span_id",
        "span.name AS name",
        "span.kind AS kind",
        "CAST(CAST(span.startTimeUnixNano AS LONG) / 1000000000 AS TIMESTAMP) AS start_time",
        "CAST(CAST(span.endTimeUnixNano AS LONG) / 1000000000 AS TIMESTAMP) AS end_time",
        "CAST((CAST(span.endTimeUnixNano AS LONG) - CAST(span.startTimeUnixNano AS LONG)) / 1000000 AS BIGINT) AS duration_ms",
        "span.status.code AS status_code",
        "to_json(resource_attrs_struct) AS resource_attrs_json",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].scopeSpans[', ss_idx, '].spans[', span_idx, '].attributes')) AS span_attrs_raw",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].scopeSpans[', ss_idx, '].spans[', span_idx, '].events')) AS span_events_raw",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].resource.attributes')) AS resource_attrs_raw",
    )

    # STRING 版：扁平化后直接存为 STRING（不走 parse_json）
    final = result.select(
        col("trace_id"),
        col("span_id"),
        col("parent_span_id"),
        col("name"),
        col("kind"),
        col("start_time"),
        col("end_time"),
        col("duration_ms"),
        col("status_code"),
        extract_otel_attr_value(col("resource_attrs_json"), lit("service.name")).alias("service_name"),
        extract_otel_attr_value(col("resource_attrs_json"), lit("service.version")).alias("service_version"),
        extract_otel_attr_value(col("resource_attrs_json"), lit("deployment.environment")).alias("deployment_env"),
        flatten_otel_attributes(col("span_attrs_raw")).alias("attributes"),
        flatten_otel_events(col("span_events_raw")).alias("events"),
        flatten_otel_attributes(col("resource_attrs_raw")).alias("resource_attributes"),
    )
    return final


# --------------------------------------------------------------------------
# 建表 / 写入 / Main
# --------------------------------------------------------------------------
def ensure_table(spark):
    namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
    if NAMESPACE not in namespaces:
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
        logger.warning("DDL 建表失败: %s", e)


def write_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        logger.info("Batch %d：空批次", batch_id)
        return
    logger.info("Batch %d：写入 %d 条到 %s", batch_id, count, TABLE_FULL_NAME)
    try:
        batch_df.writeTo(TABLE_FULL_NAME).append()
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            logger.warning("Batch %d：表不存在，尝试 create", batch_id)
            batch_df.writeTo(TABLE_FULL_NAME).tableProperty("format-version", "3").create()
        else:
            raise
    logger.info("Batch %d：写入完成", batch_id)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=(
        "boot-929.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
        "boot-hnb.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092,"
        "boot-fbo.democluster.5ay35d.c11.kafka.us-east-1.amazonaws.com:9092"
    ))
    parser.add_argument("--topic", default="agent.spans.otlp")
    parser.add_argument("--checkpoint", default="s3a://adap-prototype-812046859005/variant-shredding-test/checkpoints/otel-streaming-json/")
    parser.add_argument("--trigger-interval", default="120 seconds")
    parser.add_argument("--starting-offsets", default="latest")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("OtelStreamingJson").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("参数: topic=%s, trigger=%s", args.topic, args.trigger_interval)
    ensure_table(spark)

    raw_stream = (
        spark.readStream.format("kafka")
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

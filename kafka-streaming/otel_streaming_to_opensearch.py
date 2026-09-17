"""
otel_streaming_to_opensearch.py
================================
Spark 4.1 Structured Streaming：消费 Kafka 中的 OpenTelemetry OTLP JSON，
使用官方 OpenSearch Hadoop 2.0 connector 写入 Amazon OpenSearch Service 3.x。

写入语义：
  - Structured Streaming + checkpoint 提供 at-least-once 处理。
  - documentId = traceId:spanId，connector 使用 index 操作覆盖同 ID 文档，
    因此 micro-batch 重试或 Kafka offset 重放不会产生重复 span。
  - attributes/resourceAttributes 序列化为不索引的 JSON 文本；events 保留为 JSON 字符串，
    避免任意 OTel attribute 导致 OpenSearch mapping explosion。

前置条件：
  - 镜像包含 org.opensearch.client:opensearch-spark-40_2.13:2.0.0。
  - IAM/SigV4 模式包含 AWS SDK v2 2.31.59 或更高版本。
  - 写入前应用 kafka-streaming/opensearch-index-template.json。
"""

import argparse
import json
import logging
import os
from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_json,
    get_json_object,
    lit,
    posexplode,
    struct,
    to_json,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("OtelStreamingToOpenSearch")


# ---------------------------------------------------------------------------
# OTLP JSON schema：仅声明解析、展开和结构化索引所需的字段；未知字段会被忽略。
# ---------------------------------------------------------------------------
OTEL_ATTR_VALUE_SCHEMA = StructType([
    StructField("stringValue", StringType()),
    StructField("intValue", StringType()),
    StructField("doubleValue", DoubleType()),
    StructField("boolValue", BooleanType()),
    StructField("bytesValue", StringType()),
    StructField("arrayValue", StructType([
        StructField("values", ArrayType(StructType([
            StructField("stringValue", StringType()),
            StructField("intValue", StringType()),
            StructField("doubleValue", DoubleType()),
            StructField("boolValue", BooleanType()),
            StructField("bytesValue", StringType()),
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

OTEL_ROOT_SCHEMA = StructType([
    StructField("resourceSpans", ArrayType(StructType([
        StructField("resource", StructType([
            StructField("attributes", OTEL_KV_ARRAY_SCHEMA),
        ])),
        StructField("scopeSpans", ArrayType(SCOPE_SPANS_SCHEMA)),
    ]))),
])


def _otel_value_to_python(value: Any) -> Any:
    """把 OTLP AnyValue JSON 转换成普通 Python 值。"""
    if not isinstance(value, dict):
        return value

    for key in ("stringValue", "bytesValue", "intValue", "doubleValue", "boolValue"):
        if key in value and value[key] is not None:
            raw_value = value[key]
            if key == "intValue":
                try:
                    return int(raw_value)
                except (TypeError, ValueError):
                    return raw_value
            return raw_value

    array_value = value.get("arrayValue")
    if isinstance(array_value, dict):
        return [_otel_value_to_python(item) for item in array_value.get("values", [])]

    kvlist_value = value.get("kvlistValue")
    if isinstance(kvlist_value, dict):
        return {
            item.get("key", ""): _otel_value_to_python(item.get("value", {}))
            for item in kvlist_value.get("values", [])
            if item.get("key")
        }

    return value


@udf(MapType(StringType(), StringType(), True))
def flatten_otel_attributes(attrs_json: Optional[str]) -> Dict[str, str]:
    """
    将 OTLP key-value 数组转换成 Spark MapType(String, String)。

    标量保留其文本值；数组和对象编码成紧凑 JSON。OpenSearch 模板将该 map
    输出时再序列化为 JSON 文本，保留完整值并避免每个 attribute key 创建 mapping 字段。
    """
    if not attrs_json:
        return {}
    try:
        attrs = json.loads(attrs_json)
        if not isinstance(attrs, list):
            return {}

        flattened: Dict[str, str] = {}
        for attr in attrs:
            key = attr.get("key")
            if not key:
                continue
            value = _otel_value_to_python(attr.get("value", {}))
            if isinstance(value, (dict, list)):
                flattened[key] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            elif value is None:
                flattened[key] = None
            elif isinstance(value, bool):
                flattened[key] = "true" if value else "false"
            else:
                flattened[key] = str(value)
        return flattened
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


@udf(StringType())
def extract_otel_attr_value(attrs_json: Optional[str], wanted_key: str) -> Optional[str]:
    """从 OTLP attributes 数组中提取一个属性，供常用字段建立显式 mapping。"""
    if not attrs_json:
        return None
    try:
        attrs = json.loads(attrs_json)
        if not isinstance(attrs, list):
            return None
        for attr in attrs:
            if attr.get("key") == wanted_key:
                value = _otel_value_to_python(attr.get("value", {}))
                if isinstance(value, (dict, list)):
                    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                if value is None:
                    return None
                return str(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka OTLP JSON Structured Streaming to Amazon OpenSearch Service"
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
        help="Kafka bootstrap servers；也可设置 KAFKA_BOOTSTRAP_SERVERS",
    )
    parser.add_argument("--topic", default="agent.spans.otlp")
    parser.add_argument(
        "--checkpoint",
        default=os.getenv("OPENSEARCH_STREAM_CHECKPOINT", ""),
        help="持久化 checkpoint 路径；也可设置 OPENSEARCH_STREAM_CHECKPOINT",
    )
    parser.add_argument("--trigger-interval", default="30 seconds")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="latest")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50_000)
    parser.add_argument(
        "--opensearch-endpoint",
        default=os.getenv("OPENSEARCH_ENDPOINT", ""),
        help="Amazon OpenSearch HTTPS endpoint；也可设置 OPENSEARCH_ENDPOINT",
    )
    parser.add_argument("--opensearch-port", type=int, default=443)
    parser.add_argument("--opensearch-index", default="agent-trace-logs")
    parser.add_argument("--aws-region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--aws-service-name", choices=("es", "aoss"), default="es")
    parser.add_argument(
        "--disable-sigv4",
        action="store_true",
        help="关闭 IAM SigV4，改用 OPENSEARCH_USERNAME/OPENSEARCH_PASSWORD 基本认证",
    )
    parser.add_argument(
        "--write-partitions",
        type=int,
        default=10,
        help="每个 micro-batch 写 OpenSearch 的并发 Spark task 数",
    )
    args = parser.parse_args()

    if not args.bootstrap:
        parser.error("必须通过 --bootstrap 或 KAFKA_BOOTSTRAP_SERVERS 指定 Kafka 地址")
    if not args.opensearch_endpoint:
        parser.error("必须通过 --opensearch-endpoint 或 OPENSEARCH_ENDPOINT 指定 OpenSearch endpoint")
    if not args.checkpoint:
        parser.error("必须通过 --checkpoint 或 OPENSEARCH_STREAM_CHECKPOINT 指定持久化 checkpoint")
    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger 必须大于 0")
    if args.write_partitions <= 0:
        parser.error("--write-partitions 必须大于 0")
    return args


def parse_otel_message(raw_df):
    """将 Kafka value 中的 OTLP JSON 展开成一行一个 span 的 OpenSearch 文档。"""
    source = raw_df.selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "topic AS kafka_topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
    )

    resource_spans = source.select(
        "*",
        from_json(col("raw_json"), OTEL_ROOT_SCHEMA).alias("otel"),
    ).select(
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        posexplode(col("otel.resourceSpans")).alias("rs_idx", "rs"),
    )

    spans = resource_spans.select(
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "rs_idx",
        posexplode(col("rs.scopeSpans")).alias("ss_idx", "ss"),
    ).select(
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "rs_idx",
        "ss_idx",
        col("ss.scope.name").alias("scope_name"),
        col("ss.scope.version").alias("scope_version"),
        posexplode(col("ss.spans")).alias("span_idx", "span"),
    )

    extracted = spans.selectExpr(
        "span.traceId AS trace_id",
        "span.spanId AS span_id",
        "span.parentSpanId AS parent_span_id",
        "span.name AS span_name",
        "span.kind AS span_kind",
        "timestamp_micros(CAST(span.startTimeUnixNano AS BIGINT) DIV 1000) AS start_time",
        "timestamp_micros(CAST(span.endTimeUnixNano AS BIGINT) DIV 1000) AS end_time",
        "CAST(span.endTimeUnixNano AS BIGINT) - CAST(span.startTimeUnixNano AS BIGINT) AS duration_nanos",
        "span.status.code AS status_code",
        "scope_name",
        "scope_version",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].scopeSpans[', ss_idx, '].spans[', span_idx, '].attributes')) AS span_attrs_raw",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].scopeSpans[', ss_idx, '].spans[', span_idx, '].events')) AS span_events_raw",
        "get_json_object(raw_json, concat('$.resourceSpans[', rs_idx, '].resource.attributes')) AS resource_attrs_raw",
    ).filter(
        col("trace_id").isNotNull() & col("span_id").isNotNull()
    )

    return extracted.select(
        concat_ws(":", col("trace_id"), col("span_id")).alias("documentId"),
        col("trace_id").alias("traceId"),
        col("span_id").alias("spanId"),
        coalesce(col("parent_span_id"), lit("")).alias("parentSpanId"),
        col("span_name").alias("name"),
        col("span_kind").alias("kind"),
        col("start_time").alias("startTime"),
        col("end_time").alias("endTime"),
        col("duration_nanos").alias("durationInNanos"),
        struct(col("status_code").alias("code")).alias("status"),
        extract_otel_attr_value(col("resource_attrs_raw"), lit("service.name")).alias("serviceName"),
        extract_otel_attr_value(col("resource_attrs_raw"), lit("service.version")).alias("serviceVersion"),
        coalesce(
            extract_otel_attr_value(col("resource_attrs_raw"), lit("deployment.environment.name")),
            extract_otel_attr_value(col("resource_attrs_raw"), lit("deployment.environment")),
        ).alias("deploymentEnvironment"),
        col("scope_name").alias("scopeName"),
        col("scope_version").alias("scopeVersion"),
        extract_otel_attr_value(col("span_attrs_raw"), lit("gen_ai.operation.name")).alias("genAiOperationName"),
        extract_otel_attr_value(col("span_attrs_raw"), lit("gen_ai.agent.name")).alias("genAiAgentName"),
        extract_otel_attr_value(col("span_attrs_raw"), lit("gen_ai.request.model")).alias("genAiRequestModel"),
        extract_otel_attr_value(col("span_attrs_raw"), lit("gen_ai.usage.input_tokens")).cast("long").alias("inputTokens"),
        extract_otel_attr_value(col("span_attrs_raw"), lit("gen_ai.usage.output_tokens")).cast("long").alias("outputTokens"),
        to_json(flatten_otel_attributes(col("span_attrs_raw"))).alias("attributesJson"),
        coalesce(col("span_events_raw"), lit("[]")).alias("eventsJson"),
        to_json(flatten_otel_attributes(col("resource_attrs_raw"))).alias("resourceAttributesJson"),
        col("kafka_topic").alias("kafkaTopic"),
        col("kafka_partition").alias("kafkaPartition"),
        col("kafka_offset").alias("kafkaOffset"),
        current_timestamp().alias("ingestedAt"),
    )


def build_opensearch_options(args) -> Dict[str, str]:
    endpoint = args.opensearch_endpoint.rstrip("/")
    options = {
        "opensearch.nodes": endpoint,
        "opensearch.port": str(args.opensearch_port),
        "opensearch.net.ssl": "true" if endpoint.startswith("https://") else "false",
        "opensearch.nodes.wan.only": "true",
        "opensearch.mapping.id": "documentId",
        "opensearch.write.operation": "index",
    }

    if not args.disable_sigv4:
        options.update({
            "opensearch.aws.sigv4.enabled": "true",
            "opensearch.aws.sigv4.region": args.aws_region,
            "opensearch.aws.sigv4.service.name": args.aws_service_name,
        })
        if args.aws_service_name == "aoss":
            options["opensearch.serverless"] = "true"
    else:
        username = os.getenv("OPENSEARCH_USERNAME")
        password = os.getenv("OPENSEARCH_PASSWORD")
        if not username or not password:
            raise ValueError(
                "关闭 SigV4 时必须设置 OPENSEARCH_USERNAME 和 OPENSEARCH_PASSWORD"
            )
        options.update({
            "opensearch.net.http.auth.user": username,
            "opensearch.net.http.auth.pass": password,
        })
    return options


def write_batch(batch_df, batch_id: int, args, opensearch_options: Dict[str, str]) -> None:
    cached = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        count = cached.count()
        if count == 0:
            logger.info("Batch %d：空批次，跳过", batch_id)
            return

        logger.info(
            "Batch %d：写入 %d 个 span 到 index=%s，writePartitions=%d",
            batch_id,
            count,
            args.opensearch_index,
            args.write_partitions,
        )
        output = cached.repartition(args.write_partitions, col("traceId"))
        (
            output.write
            .format("opensearch")
            .options(**opensearch_options)
            .mode("append")
            .save(args.opensearch_index)
        )
        logger.info("Batch %d：写入完成", batch_id)
    finally:
        cached.unpersist()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("OtelStreamingToOpenSearch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "启动 OTel -> OpenSearch：topic=%s, index=%s, region=%s, sigv4=%s",
        args.topic,
        args.opensearch_index,
        args.aws_region,
        not args.disable_sigv4,
    )

    opensearch_options = build_opensearch_options(args)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("kafka.security.protocol", "PLAINTEXT")
        .load()
    )
    parsed_stream = parse_otel_message(raw_stream)

    query = (
        parsed_stream.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: write_batch(
                batch_df, batch_id, args, opensearch_options
            )
        )
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )
    logger.info("Streaming Query 已启动：id=%s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()

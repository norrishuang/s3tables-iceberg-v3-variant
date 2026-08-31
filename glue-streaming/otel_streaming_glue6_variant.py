"""
otel_streaming_glue6_variant.py
================================
AWS Glue 6.0 (Spark 4.1 + Iceberg 1.11) Structured Streaming 作业。

消费 MSK Kafka topic `agent.spans`（OpenTelemetry OTLP JSON 格式），
解析后写入 Amazon S3 Tables 的 Iceberg v3 表（VARIANT + Shredding）。

与 EKS 版本（otel_streaming_to_iceberg_v2.py）的差异：
  1. 使用 GlueContext / getResolvedOptions / Job 管理作业生命周期。
  2. Catalog 使用 AWS Glue Iceberg REST 端点访问 S3 Tables（Glue 6.0 官方推荐路径），
     也可切换为 S3TablesCatalog（通过 --catalog_impl 参数控制）。
  3. 其余 OTel 解析逻辑、目标 schema、VARIANT + Shredding 配置完全复用。

Glue 6.0 关键点：
  - Iceberg 1.11.0，完整支持 Iceberg v3（VARIANT、纳秒时间戳、地理空间类型、DEFAULT 值）。
  - VARIANT + variant shredding 通过 `spark.sql.iceberg.shred-variants=true` 与表属性
    `write.parquet.shred-variants=true` 启用。

作业参数（通过 Glue Job --arguments 传入）：
  --JOB_NAME              (Glue 必填)
  --kafka_bootstrap       MSK bootstrap servers（逗号分隔）
  --kafka_topic           Kafka topic，默认 agent.spans
  --kafka_security        安全协议：PLAINTEXT / SSL / SASL_SSL，默认 PLAINTEXT
  --starting_offsets      earliest / latest，默认 latest
  --checkpoint_location   S3 checkpoint 路径
  --trigger_interval      触发间隔，默认 "120 seconds"
  --max_offsets_per_trigger 每批最大 offset 数，默认 500000
  --warehouse_arn         S3 Tables bucket ARN
  --glue_catalog_name     Iceberg catalog 名称，默认 s3tablesbucket
  --namespace             目标 namespace，默认 tracelog
  --table_name            目标表名，默认 otel_spans_v2
  --region                AWS region，默认 us-east-1
"""

import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, from_json, lit, parse_json, posexplode, udf,
)
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("OtelStreamingGlue6Variant")

# --------------------------------------------------------------------------
# OTel JSON Schema（用于 from_json 解析）
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
# UDF：将 OTel attributes / events 转为扁平 map JSON（对 Parquet Shredding 友好）
# --------------------------------------------------------------------------
@udf(StringType())
def flatten_otel_attributes(attrs_json: str) -> str:
    """将 [{"key":"k","value":{"stringValue":"v"}},...] 转为 {"k":"v",...}"""
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
    """将 events 数组中的 attributes 也扁平化"""
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
# 参数解析（Glue getResolvedOptions）
# --------------------------------------------------------------------------
def resolve_args():
    # 先取必填 + 全部可选参数名；不存在的可选参数由 Glue 用默认覆盖
    optional_defaults = {
        "kafka_topic": "agent.spans",
        "kafka_security": "PLAINTEXT",
        "starting_offsets": "latest",
        "trigger_interval": "120 seconds",
        "max_offsets_per_trigger": "500000",
        "glue_catalog_name": "s3tablesbucket",
        "namespace": "tracelog",
        "table_name": "otel_spans_v2",
        "region": "us-east-1",
        "catalog_impl": "rest",  # rest（Glue Iceberg REST 端点）或 s3tables（S3TablesCatalog）
    }
    required = [
        "JOB_NAME",
        "kafka_bootstrap",
        "checkpoint_location",
        "warehouse_arn",
    ]
    # 收集实际传入的可选参数（getResolvedOptions 对缺失的可选参数会报错，需先探测）
    provided_optional = [k for k in optional_defaults if f"--{k}" in sys.argv]
    args = getResolvedOptions(sys.argv, required + provided_optional)
    for k, v in optional_defaults.items():
        args.setdefault(k, v)
    return args


# --------------------------------------------------------------------------
# 建表
# --------------------------------------------------------------------------
def build_ddl(table_full_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_full_name} (
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
        'write.parquet.compression-codec' = 'zstd',
        'write.parquet.shred-variants' = 'true'
    )
    """


def ensure_table(spark, catalog, namespace, table_name, table_full_name):
    namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]
    if namespace not in namespaces:
        logger.info("创建 namespace %s", namespace)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    tables = [row[1] for row in spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()]
    if table_name in tables:
        logger.info("目标表 %s 已存在", table_full_name)
        return
    logger.info("执行建表 DDL……")
    spark.sql(build_ddl(table_full_name))
    logger.info("目标表 %s 创建成功", table_full_name)


# --------------------------------------------------------------------------
# 解析 OTel JSON → 目标 schema
# --------------------------------------------------------------------------
def parse_otel_message(raw_df):
    df = raw_df.selectExpr("CAST(value AS STRING) AS raw_json")

    parsed = df.select(
        col("raw_json"),
        from_json(col("raw_json"), OTEL_ROOT_SCHEMA).alias("otel"),
    ).select(
        col("raw_json"),
        posexplode(col("otel.resourceSpans")).alias("rs_idx", "rs"),
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
        parse_json(flatten_otel_attributes(col("span_attrs_raw"))).alias("attributes"),
        parse_json(flatten_otel_events(col("span_events_raw"))).alias("events"),
        parse_json(flatten_otel_attributes(col("resource_attrs_raw"))).alias("resource_attributes"),
    )
    return final


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    args = resolve_args()

    catalog = args["glue_catalog_name"]
    namespace = args["namespace"]
    table_name = args["table_name"]
    table_full_name = f"{catalog}.{namespace}.{table_name}"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "参数: topic=%s, security=%s, offsets=%s, trigger=%s, table=%s, catalog_impl=%s",
        args["kafka_topic"], args["kafka_security"], args["starting_offsets"],
        args["trigger_interval"], table_full_name, args["catalog_impl"],
    )

    # 注册 UDF
    spark.udf.register("flatten_otel_attributes", flatten_otel_attributes)
    spark.udf.register("extract_otel_attr_value", extract_otel_attr_value)
    spark.udf.register("flatten_otel_events", flatten_otel_events)

    # 建表
    ensure_table(spark, catalog, namespace, table_name, table_full_name)

    # Kafka source
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args["kafka_bootstrap"])
        .option("subscribe", args["kafka_topic"])
        .option("startingOffsets", args["starting_offsets"])
        .option("maxOffsetsPerTrigger", int(args["max_offsets_per_trigger"]))
        .option("kafka.security.protocol", args["kafka_security"])
    )
    # MSK IAM 认证时使用 SASL_SSL（如需，可在 --kafka_security SASL_SSL 时附加）
    if args["kafka_security"] == "SASL_SSL":
        reader = (
            reader
            .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
            .option("kafka.sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required;")
            .option("kafka.sasl.client.callback.handler.class",
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        )

    raw_stream = reader.load()
    parsed_stream = parse_otel_message(raw_stream)

    def write_batch(batch_df, batch_id):
        count = batch_df.count()
        if count == 0:
            logger.info("Batch %d：空批次，跳过", batch_id)
            return
        logger.info("Batch %d：写入 %d 条到 %s", batch_id, count, table_full_name)
        batch_df.writeTo(table_full_name).append()
        logger.info("Batch %d：写入完成", batch_id)

    query = (
        parsed_stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", args["checkpoint_location"])
        .trigger(processingTime=args["trigger_interval"])
        .start()
    )

    logger.info("Streaming Query 已启动")
    query.awaitTermination()

    job.commit()


if __name__ == "__main__":
    main()

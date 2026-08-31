#!/usr/bin/env bash
#
# create_glue_job.sh
# ==================
# 创建（或更新）运行 otel_streaming_glue6_variant.py 的 AWS Glue 6.0 Streaming 作业。
#
# 前置：
#   1. 已把脚本上传到 S3：
#      aws s3 cp glue-streaming/otel_streaming_glue6_variant.py s3://<bucket>/glue-scripts/
#   2. 有一个具备访问 MSK / S3 Tables / Glue / S3 权限的 IAM Role。
#
# Glue 6.0 关键配置（通过 --default-arguments 传入 Spark conf）：
#   - Iceberg 1.11 + Iceberg v3（VARIANT / shredding）
#   - S3 Tables 通过 AWS Glue Iceberg REST 端点访问（type=rest, sigv4）
#   - spark.sql.iceberg.shred-variants=true 启用 variant shredding 写入路径
#
set -euo pipefail

# -------------------- 需按环境修改 --------------------
REGION="us-east-1"
ACCOUNT_ID="812046859005"
JOB_NAME="otel-streaming-glue6-variant"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/<GLUE_JOB_ROLE>"        # ← 修改
SCRIPT_S3="s3://adap-prototype-${ACCOUNT_ID}/glue-scripts/otel_streaming_glue6_variant.py"
CHECKPOINT_S3="s3://adap-prototype-${ACCOUNT_ID}/glue-streaming/checkpoints/otel-glue6-variant/"
WAREHOUSE_ARN="arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/iceberg-data-${ACCOUNT_ID}"

KAFKA_BOOTSTRAP="boot-oej.ebs3tablesdemo.bv5co8.c1.kafka.us-east-1.amazonaws.com:9092,boot-wfh.ebs3tablesdemo.bv5co8.c1.kafka.us-east-1.amazonaws.com:9092,boot-idd.ebs3tablesdemo.bv5co8.c1.kafka.us-east-1.amazonaws.com:9092"
KAFKA_TOPIC="agent.spans"
CATALOG_NAME="s3tablesbucket"
NAMESPACE="tracelog"
TABLE_NAME="otel_spans_v2"
# ------------------------------------------------------

# S3 Tables 通过 Iceberg REST 端点访问
S3TABLES_REST_URI="https://s3tables.${REGION}.amazonaws.com/iceberg"

# Spark / Iceberg conf（用 --conf 传给 Glue，多条以空格分隔于同一字符串）
SPARK_CONF="spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.rest.RESTCatalog"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.uri=${S3TABLES_REST_URI}"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_ARN}"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.rest.sigv4-enabled=true"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.rest.signing-name=s3tables"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.rest.signing-region=${REGION}"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.defaultCatalog=${CATALOG_NAME}"
SPARK_CONF="${SPARK_CONF} --conf spark.sql.iceberg.shred-variants=true"

aws glue create-job \
  --region "${REGION}" \
  --name "${JOB_NAME}" \
  --role "${ROLE_ARN}" \
  --glue-version "6.0" \
  --worker-type "G.2X" \
  --number-of-workers 10 \
  --command "Name=gluestreaming,ScriptLocation=${SCRIPT_S3},PythonVersion=3" \
  --default-arguments "{
    \"--job-language\":\"python\",
    \"--datalake-formats\":\"iceberg\",
    \"--enable-metrics\":\"true\",
    \"--enable-continuous-cloudwatch-log\":\"true\",
    \"--conf\":\"${SPARK_CONF}\",
    \"--kafka_bootstrap\":\"${KAFKA_BOOTSTRAP}\",
    \"--kafka_topic\":\"${KAFKA_TOPIC}\",
    \"--kafka_security\":\"PLAINTEXT\",
    \"--starting_offsets\":\"earliest\",
    \"--trigger_interval\":\"120 seconds\",
    \"--max_offsets_per_trigger\":\"500000\",
    \"--checkpoint_location\":\"${CHECKPOINT_S3}\",
    \"--warehouse_arn\":\"${WAREHOUSE_ARN}\",
    \"--glue_catalog_name\":\"${CATALOG_NAME}\",
    \"--namespace\":\"${NAMESPACE}\",
    \"--table_name\":\"${TABLE_NAME}\",
    \"--region\":\"${REGION}\"
  }"

echo "Glue job ${JOB_NAME} created. Start with:"
echo "  aws glue start-job-run --region ${REGION} --job-name ${JOB_NAME}"

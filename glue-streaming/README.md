# Glue 6.0 Streaming → S3 Tables (Iceberg v3 VARIANT + Shredding)

基于 EKS 版本（`kafka-streaming/otel_streaming_to_iceberg_v2.py`）改写，运行在 **AWS Glue 6.0**
（Spark 4.1 + Iceberg 1.11）上，消费 MSK 中的 OTel trace log（`agent.spans`），写入 S3 Tables 的
Iceberg v3 VARIANT 表（带 shredding）。

## 为什么能跑在 Glue 6.0

Glue 6.0 升级到 **Iceberg 1.11.0，完整支持 Iceberg v3**：
- VARIANT 数据类型 + variant shredding
- 纳秒精度时间戳
- 地理空间类型（Geometry / Geography）
- DEFAULT 列值

Iceberg 版本与我们的 EKS 镜像（`iceberg1.11`）一致，因此 VARIANT + shredding 的解析/写入逻辑可直接复用。

## 文件

| 文件 | 说明 |
|------|------|
| `otel_streaming_glue6_variant.py` | Glue 6.0 Structured Streaming 主脚本 |
| `create_glue_job.sh` | 创建 Glue Streaming 作业的 CLI 脚本（含正确的 catalog/shredding conf） |

## 与 EKS 版本的差异

1. 用 `GlueContext` / `getResolvedOptions` / `Job` 管理生命周期，而非裸 `SparkSession`。
2. S3 Tables 通过 **Iceberg REST 端点**（`type=rest` + sigv4）访问 —— Glue 上比 `S3TablesCatalog`
   JAR 更省事。若要用 `S3TablesCatalog`，需把该 JAR 通过 `--extra-jars` 挂到作业。
3. 所有 OTel 解析逻辑、目标 schema、`write.parquet.shred-variants=true`、`spark.sql.iceberg.shred-variants=true`
   完全复用。

## Variant Shredding 配置（关键）

| 层级 | 配置 |
|------|------|
| Session | `spark.sql.iceberg.shred-variants=true` |
| 表属性 | `write.parquet.shred-variants=true` |

> 注意：`spark.sql.parquet.variantShreddingEnabled` 是 Spark 原生 Parquet 写入路径配置，**不适用**于
> Iceberg 写入路径。

## 部署步骤

```bash
# 1. 上传脚本
aws s3 cp glue-streaming/otel_streaming_glue6_variant.py \
  s3://adap-prototype-812046859005/glue-scripts/

# 2. 编辑 create_glue_job.sh 中的 ROLE_ARN，再创建作业
bash glue-streaming/create_glue_job.sh

# 3. 启动
aws glue start-job-run --region us-east-1 --job-name otel-streaming-glue6-variant

# 4. 查看日志（CloudWatch）
#    /aws-glue/jobs/logs-v2  →  <job-run-id>
```

## 目标表 schema

```
tracelog.otel_spans_v2
├── 结构化列：trace_id, span_id, parent_span_id, name, kind,
│             start_time, end_time, duration_ms, status_code,
│             service_name, service_version, deployment_env
├── VARIANT + Shredding：attributes, events, resource_attributes
└── 分区：days(start_time)
```

## IAM 权限要点

作业 Role 需要：
- `s3tables:*`（对目标 table bucket）—— 建表/写入
- `glue:*`（若走 Glue Data Catalog）
- `s3:GetObject/PutObject`（脚本、checkpoint、S3 Tables 底层数据）
- MSK 访问：VPC 内连接 + 若用 IAM 认证则 `kafka-cluster:*`（当前示例用 PLAINTEXT，只需网络可达）

## 网络

Glue Streaming 作业需通过 **Glue Connection（NETWORK 类型）** 接入 MSK 所在的 VPC/子网/安全组，
否则无法连到 broker。创建 job 时用 `--connections <connection-name>` 关联。

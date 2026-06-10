# S3 Tables Iceberg V3 Variant Shredding 实验

## 目标

使用 **开源 Spark 4.1.2 + EKS** 测试 Amazon S3 Tables 对 **Apache Iceberg V3 Variant 类型（含 Shredding）** 的读写支持，并与 EMR Serverless（Spark 3.5，无 Shredding）做性能横向对比。

## 环境

| 组件 | 版本/规格 |
|------|-----------|
| Spark | 4.1.2（开源，非 EMR 镜像） |
| Iceberg | 1.11.0 |
| Java | 17 (Amazon Corretto) |
| EKS Namespace | `emr-eks-spark` |
| S3 Tables Bucket (源) | `arn:aws:s3tables:us-east-1:812046859005:bucket/iceberg-data-812046859005` |
| S3 Tables Bucket (目标) | `arn:aws:s3tables:us-east-1:812046859005:bucket/demo-tb-bucket-812046859005` |
| Catalog | REST Catalog + SigV4 (`https://s3tables.us-east-1.amazonaws.com/iceberg`) |
| ECR Registry | `812046859005.dkr.ecr.us-east-1.amazonaws.com` |
| ServiceAccount | `emr-job-execution-sa` (IRSA) |

## 项目结构

```
.
├── docker/
│   ├── Dockerfile                  # OSS Spark 4.1.2 镜像（Java 17，无 Python）
│   ├── entrypoint-wrapper.sh       # 过滤 EMR Operator 注入的类名和路径
│   └── spark-submit-wrapper.sh     # spark-submit 包装脚本
│
├── kafka-streaming/
│   ├── Dockerfile                  # Spark + Python 3.11 + Kafka + S3TablesCatalog 镜像
│   ├── kafka-streaming.yaml        # SparkApplication YAML（Spark Operator）
│   └── kafka_streaming_to_iceberg.py  # PySpark Structured Streaming 脚本
│
├── k8s/
│   ├── variant-test-etl-java.yaml  # 主 ETL Job（40亿行全量写入）
│   ├── variant-shredding-setup.yaml# 建表 + 数据生成 Job（1000万行模拟数据）
│   └── variant-shredding-perf.yaml # 性能测试 Job（Q1-Q4 对比）
│
├── etl/
│   ├── java/                       # Java ETL（唯一运行版本）
│   │   ├── pom.xml
│   │   └── src/main/java/com/example/
│   │       ├── ShredingEtl.java    # 主 ETL：源表 → 2 张 Shredding 目标表
│   │       ├── SetupTables.java    # 建表 + 模拟数据生成
│   │       └── PerfTest.java       # 性能测试（Q1-Q4）
│   ├── deprecated/                 # 旧 PySpark 脚本（仅供参考，不再使用）
│   └── emr-serverless/             # EMR Serverless 基准测试脚本（对比用）
│
└── docs/
    └── (实验记录、问题排查笔记)
```

## 关键技术决策

### 1. 为什么用 Java 而不是 PySpark
PySpark 在 EKS 上会触发 `Py4J ReflectionCommand`，扫描 641MB 的 `aws-java-sdk-bundle` 时 Thread-5 CPU 飙升到 460s+，永远不会结束。Java 直接构建 SparkSession，完全绕开此问题。

### 2. REST Catalog vs S3TablesCatalog

**重要发现**：Iceberg REST Catalog（`org.apache.iceberg.rest.RESTCatalog`）**不支持**通过 Spark DDL 创建 `format-version = 3` 的表。执行 `CREATE TABLE ... TBLPROPERTIES ('format-version' = '3')` 时，S3 Tables REST API 会返回：

```
org.apache.iceberg.exceptions.BadRequestException: Malformed request: The specified metadata is not valid
```

这是因为 S3 Tables 的 Iceberg REST endpoint 在处理 format-version=3 的 table metadata（包含 VARIANT 类型定义）时不兼容。

**解决方案**：

| 操作 | 使用的 Catalog 实现 | 说明 |
|------|---------------------|------|
| **建表（DDL）** | `software.amazon.s3tables.iceberg.S3TablesCatalog` | 需要 `s3-tables-catalog-for-iceberg-runtime` JAR |
| **读写数据** | 两者均可 | REST Catalog 对已有表的读写正常 |

在需要建表的场景（如 Streaming 程序需要自动建表），使用 S3TablesCatalog：
```yaml
spark.sql.catalog.s3tablesbucket: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.s3tablesbucket.catalog-impl: software.amazon.s3tables.iceberg.S3TablesCatalog
spark.sql.catalog.s3tablesbucket.warehouse: arn:aws:s3tables:us-east-1:812046859005:bucket/<bucket-name>
spark.sql.catalog.s3tablesbucket.client.region: us-east-1
```

在只做读写（表已存在）的场景，REST Catalog 仍然可用：
```yaml
spark.sql.catalog.s3tablesbucket: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.s3tablesbucket.catalog-impl: org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.s3tablesbucket.uri: https://s3tables.us-east-1.amazonaws.com/iceberg
spark.sql.catalog.s3tablesbucket.rest.auth.type: sigv4
spark.sql.catalog.s3tablesbucket.rest.signing-name: s3tables
spark.sql.catalog.s3tablesbucket.rest.signing-region: us-east-1
spark.sql.catalog.s3tablesbucket.warehouse: arn:aws:s3tables:us-east-1:812046859005:bucket/<bucket-name>
```

### 3. EMR Operator MutatingWebhook 注入问题
EKS 集群上运行的 EMR on EKS Operator 会注入 EMR 专有的 extraClassPath 和 JAVA_TOOL_OPTIONS，导致开源 Spark 启动失败。通过三层防御解决：
1. `entrypoint-wrapper.sh` 过滤 spark.properties 中的 EMR 配置
2. `spark-submit-wrapper.sh` 重定向 --properties-file
3. emptyDir volume 覆盖 `/usr/lib/spark/conf` 挂载点

### 4. SDK JAR 冲突
避免同时存在 `bundle-2.24.6.jar`（来自 tools/lib）和任何其他版本 bundle。只保留单一 `aws-java-sdk-bundle-2.29.52.jar`。

## 构建

### 镜像构建

```bash
cd docker/

# 登录 ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 812046859005.dkr.ecr.us-east-1.amazonaws.com

# 构建并推送
docker build -t 812046859005.dkr.ecr.us-east-1.amazonaws.com/oss-spark:4.1.2-iceberg1.11-s3t .
docker push 812046859005.dkr.ecr.us-east-1.amazonaws.com/oss-spark:4.1.2-iceberg1.11-s3t
```

### Java ETL 构建

```bash
cd etl/java/
mvn clean package -DskipTests

# 上传 JAR 到 S3
aws s3 cp target/shredding-etl-1.0.0-shaded.jar \
  s3://adap-prototype-812046859005/variant-shredding-test/jars/
```

## 运行

### Step 1: 建表 + 模拟数据（可选，用于性能对比测试）

```bash
kubectl apply -f k8s/variant-shredding-setup.yaml -n emr-eks-spark
```

### Step 2: 性能测试

```bash
kubectl apply -f k8s/variant-shredding-perf.yaml -n emr-eks-spark
```

### Step 3: 主 ETL（40 亿行全量写入）

```bash
kubectl apply -f k8s/variant-test-etl-java.yaml -n emr-eks-spark
kubectl logs -f <driver-pod-name> -n emr-eks-spark
```

## ETL 说明

### ShredingEtl（主 ETL）

- 源表：`srccat.gamedb.game_action_logs`（~40 亿行，iceberg-data bucket）
- 目标表：
  - `dstcat.iceberg_v3_test.variant_test_no_partition` — hours(tm) 分区
  - `dstcat.iceberg_v3_test.variant_test_with_bucket` — hours(tm) + bucket(64,aid) 分区
- 字段映射：`log_id→event_id, aid→aid, tm→tm, parse_json(detail)→detail`
- 按天分批处理（32 天），支持断点续跑
- 使用 `MEMORY_AND_DISK` 缓存策略，每天数据 cache 后写两张表

### SetupTables（建表 + 模拟数据）

- 创建 `variant_no_bucket` 和 `variant_with_bucket` 两张表
- 生成 1000 万行模拟数据（与 EMR Serverless 测试对齐）
- 用于性能对比测试

### PerfTest（性能测试）

- Q1: 单 aid 精确查询（验证 bucket pruning）
- Q2: 全表扫描聚合（验证 shredding 列统计下推）
- Q3: 多 aid 过滤（中间场景）
- Q4: 嵌套字段聚合（验证 shredding 对嵌套字段效果）
- 每个查询跑 3 次取均值，对比 no_bucket vs with_bucket

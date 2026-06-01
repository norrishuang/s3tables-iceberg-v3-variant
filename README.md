# S3 Tables Iceberg V3 Variant Shredding 实验

## 目标

使用 **开源 Spark 4.1.2 + EKS** 测试 Amazon S3 Tables 对 **Apache Iceberg V3 Variant 类型（含 Shredding）** 的读写支持，并与 EMR Serverless（Spark 3.5，无 Shredding）做性能横向对比。

## 环境

| 组件 | 版本/规格 |
|------|-----------|
| Spark | 4.1.2（开源，非 EMR 镜像） |
| Iceberg | 1.11.0 |
| EKS Namespace | `emr-eks-spark` |
| S3 Tables Bucket | `arn:aws:s3tables:us-east-1:812046859005:bucket/iceberg-data-812046859005` |
| 目标表 | `gamedb.game_action_logs` / `gamedb.game_action_logs_variant` |
| Catalog | REST Catalog + SigV4 (`https://s3tables.us-east-1.amazonaws.com/iceberg`) |
| ECR Registry | `812046859005.dkr.ecr.us-east-1.amazonaws.com` |
| ServiceAccount | `emr-job-execution-sa` (IRSA) |

## 项目结构

```
.
├── docker/
│   ├── Dockerfile                  # OSS Spark 4.1.2 镜像（最终稳定版，v13+）
│   ├── entrypoint-wrapper.sh       # 过滤 EMR Operator 注入的类名和路径
│   └── spark-submit-wrapper.sh     # spark-submit 包装脚本
│
├── k8s/
│   ├── variant-test-etl-java.yaml  # 主 ETL Job（Java，SparkApplication CRD）
│   ├── variant-shredding-etl.yaml  # 旧 PySpark ETL Job（已弃用，留档）
│   ├── variant-shredding-perf.yaml # 性能测试 Job
│   └── variant-shredding-setup.yaml# 建表 Job
│
├── etl/
│   ├── java/                       # Java ETL（推荐，避免 Py4J 反射卡死）
│   │   ├── pom.xml
│   │   └── src/main/java/com/example/ShredingEtl.java
│   ├── etl_shredding_from_source.py # PySpark ETL（旧版，供参考）
│   ├── etl_variant_test_fullload.py # PySpark 全量加载脚本
│   └── emr-serverless/             # EMR Serverless 基准测试脚本
│       ├── setup_variant_shredding_tables.py
│       └── perf_test_variant_shredding.py
│
└── docs/
    └── (实验记录、问题排查笔记)
```

## 关键技术决策

### 1. 为什么用 Java 而不是 PySpark
PySpark 在 EKS 上会触发 `Py4J ReflectionCommand`，扫描 641MB 的 `aws-java-sdk-bundle` 时 Thread-5 CPU 飙升到 460s+，永远不会结束。Java 直接构建 SparkSession，完全绕开此问题。

### 2. REST Catalog（不是 s3-tables-catalog-for-iceberg）
`s3-tables-catalog-for-iceberg` 只支持 Spark 3.x，Spark 4.x 必须使用 REST Catalog + SigV4 签名。

关键配置：
```yaml
spark.sql.catalog.s3tablesbucket: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.s3tablesbucket.catalog-impl: org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.s3tablesbucket.uri: https://s3tables.us-east-1.amazonaws.com/iceberg
spark.sql.catalog.s3tablesbucket.rest.sigv4-enabled: "true"
spark.sql.catalog.s3tablesbucket.rest.signing-region: us-east-1
spark.sql.catalog.s3tablesbucket.rest.signing-name: s3tables
spark.sql.catalog.s3tablesbucket.warehouse: arn:aws:s3tables:us-east-1:812046859005:bucket/iceberg-data-812046859005
```

### 3. EMR Operator MutatingWebhook 注入问题
EKS 集群上运行的 EMR on EKS Operator 会注入 EMR 专有的 extraClassPath 和 JAVA_TOOL_OPTIONS，导致开源 Spark 启动失败。`entrypoint-wrapper.sh` 过滤掉这些注入。

### 4. SDK JAR 冲突
避免同时存在 `bundle-2.24.6.jar`（来自 tools/lib）和任何其他版本 bundle。只保留单一 `aws-java-sdk-bundle-2.29.52.jar`。

## 镜像构建

```bash
cd docker/

# 登录 ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 812046859005.dkr.ecr.us-east-1.amazonaws.com

# 构建并推送
docker build -t 812046859005.dkr.ecr.us-east-1.amazonaws.com/spark-iceberg-variant:latest .
docker push 812046859005.dkr.ecr.us-east-1.amazonaws.com/spark-iceberg-variant:latest
```

## Java ETL 构建

```bash
cd etl/java/
mvn clean package -DskipTests
# 将 target/shredding-etl-1.0.0-shaded.jar 上传到 S3 或打入镜像
```

## 提交 Job

```bash
kubectl apply -f k8s/variant-test-etl-java.yaml -n emr-eks-spark
kubectl logs -f <driver-pod-name> -n emr-eks-spark
```

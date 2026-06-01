"""
Iceberg V3 VARIANT Shredding 对比实验 — 建表 + 数据生成
运行环境: EKS + Spark 4.1.2（开源）+ Iceberg 1.11.0

表设计：
  variant_no_bucket    — VARIANT payload，无分桶，format-version=3
  variant_with_bucket  — VARIANT payload，BUCKET(64,aid) 分桶，format-version=3

两张表启用 Shredding（spark.sql.parquet.variantShreddingEnabled=true），
与上次 EMR Serverless 测试（Spark 3.5，无 Shredding）形成横向对比。

数据：模拟电商/游戏行为日志，1000 万行，每行含：
  - aid (STRING)：用户 ID，10 个不同值（与上次测试一致）
  - 查询场景：单 aid 聚合、全表扫描
"""

import logging
import sys
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 配置常量（与上次 EMR 测试对齐）
# ============================================================
ACCOUNT_ID = "812046859005"
REGION = "us-east-1"
TABLE_BUCKET_ARN = f"arn:aws:s3tables:{REGION}:{ACCOUNT_ID}:bucket/demo-tb-bucket-812046859005"
CATALOG = "s3tablesbucket"
NAMESPACE = "variant_shredding_test"

# 与上次性能测试相同的 10 个 aid 值
AIDS = [
    "aid_001", "aid_002", "aid_003", "aid_004", "aid_005",
    "aid_006", "aid_007", "aid_008", "aid_009", "aid_010"
]

# 目标行数（与上次测试数据量对齐）
TARGET_ROWS = 10_000_000

TABLE_NO_BUCKET  = f"{NAMESPACE}.variant_no_bucket"
TABLE_WITH_BUCKET = f"{NAMESPACE}.variant_with_bucket"

# 输出结果路径（用于保存建表日志）
RESULT_BUCKET = f"s3://adap-prototype-{ACCOUNT_ID}"
RESULT_PREFIX = "variant-shredding-test/setup-result"


def create_spark_session():
    spark = (SparkSession.builder
        .appName("Variant Shredding Tables Setup")
        # ── S3 Tables Catalog ──
        .config("spark.sql.catalog.s3tablesbucket",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config("spark.sql.catalog.s3tablesbucket.warehouse", TABLE_BUCKET_ARN)
        .config("spark.sql.catalog.s3tablesbucket.client.region", REGION)
        .config("spark.sql.defaultCatalog", CATALOG)
        # ── Iceberg 扩展 ──
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # ── VARIANT Shredding（Spark 4.1 新特性）──
        .config("spark.sql.parquet.variantShreddingEnabled", "true")
        # ── S3A 访问（IRSA）──
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        # ── 性能 ──
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark 版本: {spark.version}")
    logger.info(f"Shredding 配置: {spark.conf.get('spark.sql.parquet.variantShreddingEnabled')}")
    return spark


def drop_and_create_tables(spark):
    """删除旧表，重建两张对比表"""
    logger.info("=== 创建 Namespace ===")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    logger.info("=== 删除旧表（如存在）===")
    for tbl in [TABLE_NO_BUCKET, TABLE_WITH_BUCKET]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {tbl} PURGE")
            logger.info(f"  已删除: {tbl}")
        except Exception as e:
            logger.info(f"  跳过删除（不存在）: {tbl} — {e}")

    logger.info("=== 创建 variant_no_bucket（无分桶）===")
    spark.sql(f"""
        CREATE TABLE {TABLE_NO_BUCKET} (
            row_id   BIGINT,
            aid      STRING,
            event_ts TIMESTAMP,
            payload  VARIANT
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'               = '3',
            'write.format.default'         = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info(f"  已创建: {TABLE_NO_BUCKET}")

    logger.info("=== 创建 variant_with_bucket（BUCKET(64, aid)）===")
    spark.sql(f"""
        CREATE TABLE {TABLE_WITH_BUCKET} (
            row_id   BIGINT,
            aid      STRING,
            event_ts TIMESTAMP,
            payload  VARIANT
        )
        USING iceberg
        PARTITIONED BY (BUCKET(64, aid))
        TBLPROPERTIES (
            'format-version'               = '3',
            'write.format.default'         = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info(f"  已创建: {TABLE_WITH_BUCKET}")


def generate_and_write(spark):
    """生成 1000 万行模拟数据，写入两张表"""
    logger.info(f"=== 生成 {TARGET_ROWS:,} 行数据 ===")

    # 用 Spark 原生方式生成大数据量（避免 Python 循环慢）
    # 先生成骨架 DataFrame，再用 SQL 函数构造 VARIANT payload
    df_base = (
        spark.range(TARGET_ROWS)
        .withColumn("aid",
            F.element_at(
                F.array([F.lit(a) for a in AIDS]),
                (F.col("id") % len(AIDS) + 1).cast("int")
            )
        )
        .withColumn("event_ts",
            (F.lit(datetime(2024, 1, 1).timestamp()) +
             F.rand(seed=42) * 365 * 86400
            ).cast("timestamp")
        )
        .withColumn("event_type",
            F.element_at(
                F.array(
                    F.lit("click"), F.lit("purchase"), F.lit("view"),
                    F.lit("search"), F.lit("add_cart"), F.lit("checkout")
                ),
                (F.col("id") % 6 + 1).cast("int")
            )
        )
        .withColumn("price",     (F.rand(seed=1) * 999 + 1).cast("decimal(10,2)"))
        .withColumn("quantity",  (F.rand(seed=2) * 10 + 1).cast("int"))
        .withColumn("category",
            F.element_at(
                F.array(
                    F.lit("electronics"), F.lit("clothing"), F.lit("food"),
                    F.lit("sports"), F.lit("books")
                ),
                (F.col("id") % 5 + 1).cast("int")
            )
        )
        .withColumn("score",     (F.rand(seed=3) * 100).cast("decimal(5,2)"))
        .withColumn("session_id", F.concat(F.lit("sess_"), F.col("id").cast("string")))
        # 构造 JSON 字符串，再用 parse_json 转 VARIANT
        .withColumn("payload_json", F.to_json(F.struct(
            F.col("aid"),
            F.col("event_type"),
            F.col("event_ts").cast("string").alias("ts"),
            F.col("price"),
            F.col("quantity"),
            F.col("category"),
            F.col("score"),
            F.col("session_id"),
            F.struct(
                (F.rand(seed=4) * 180 - 90).cast("decimal(8,5)").alias("lat"),
                (F.rand(seed=5) * 360 - 180).cast("decimal(8,5)").alias("lon")
            ).alias("geo")
        )))
        .withColumn("payload", F.parse_json(F.col("payload_json")))
        .select(
            F.col("id").alias("row_id"),
            F.col("aid"),
            F.col("event_ts"),
            F.col("payload")
        )
    )

    logger.info("  数据生成完毕，开始写入 variant_no_bucket ...")
    df_base.writeTo(TABLE_NO_BUCKET).append()
    cnt_no = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_NO_BUCKET}").collect()[0]["c"]
    logger.info(f"  variant_no_bucket: {cnt_no:,} 行")

    logger.info("  开始写入 variant_with_bucket ...")
    df_base.writeTo(TABLE_WITH_BUCKET).append()
    cnt_with = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_WITH_BUCKET}").collect()[0]["c"]
    logger.info(f"  variant_with_bucket: {cnt_with:,} 行")

    return cnt_no, cnt_with


def verify(spark):
    """简单抽样验证数据正确性"""
    logger.info("=== 验证数据 ===")
    for tbl in [TABLE_NO_BUCKET, TABLE_WITH_BUCKET]:
        logger.info(f"-- {tbl} --")
        spark.sql(f"""
            SELECT
                aid,
                COUNT(*) AS cnt,
                COUNT(DISTINCT variant_get(payload, '$.session_id', 'STRING')) AS uniq_sessions,
                AVG(variant_get(payload, '$.price', 'DOUBLE')) AS avg_price
            FROM {tbl}
            GROUP BY aid
            ORDER BY aid
        """).show()


def main():
    spark = create_spark_session()
    try:
        logger.info("===== STEP 1: 建表 =====")
        drop_and_create_tables(spark)

        logger.info("===== STEP 2: 数据生成 =====")
        cnt_no, cnt_with = generate_and_write(spark)

        logger.info("===== STEP 3: 验证 =====")
        verify(spark)

        logger.info("===== 建表完成 =====")
        logger.info(f"  variant_no_bucket  : {cnt_no:,} 行，无分桶")
        logger.info(f"  variant_with_bucket: {cnt_with:,} 行，BUCKET(64,aid)")
        logger.info(f"  Shredding: 已开启（Spark 4.1.2）")
    except Exception as e:
        logger.error(f"失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Iceberg V3 VARIANT Shredding 性能测试脚本
运行环境: EKS + Spark 4.1.2（开源）+ Iceberg 1.11.0

测试维度：
  Q1  — 单 aid 精确查询（点查，验证 bucket pruning）
  Q2  — 全表扫描聚合（全量，验证 shredding 带来的列统计下推）
  Q3  — 多 aid 过滤（范围，中间场景）
  Q4  — aid 内嵌套字段聚合（验证 shredding 对嵌套字段的效果）

每个查询在 variant_no_bucket / variant_with_bucket 两张表上各跑 3 次，取均值。
结果对比：
  - no_bucket vs with_bucket：bucket pruning 效果
  - 与上次 EMR（无 Shredding）比较：shredding 对全表扫描的提升
"""

import logging
import time
import json
import sys
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ACCOUNT_ID = "812046859005"
REGION = "us-east-1"
TABLE_BUCKET_ARN = f"arn:aws:s3tables:{REGION}:{ACCOUNT_ID}:bucket/demo-tb-bucket-812046859005"
CATALOG = "s3tablesbucket"
NAMESPACE = "variant_shredding_test"

TABLE_NO_BUCKET   = f"{NAMESPACE}.variant_no_bucket"
TABLE_WITH_BUCKET = f"{NAMESPACE}.variant_with_bucket"

# 与上次 EMR 测试相同的 10 个 aid，逐一测试
AIDS = [
    "aid_001", "aid_002", "aid_003", "aid_004", "aid_005",
    "aid_006", "aid_007", "aid_008", "aid_009", "aid_010"
]


def create_spark_session():
    spark = (SparkSession.builder
        .appName("Variant Shredding Performance Test")
        .config("spark.sql.catalog.s3tablesbucket",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config("spark.sql.catalog.s3tablesbucket.warehouse", TABLE_BUCKET_ARN)
        .config("spark.sql.catalog.s3tablesbucket.client.region", REGION)
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Shredding 开启
        .config("spark.sql.parquet.variantShreddingEnabled", "true")
        # S3A
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        # 性能
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark 版本: {spark.version}")
    return spark


def run_query(spark, sql, label):
    start = time.time()
    df = spark.sql(sql)
    df.collect()
    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(f"  [{label}] {elapsed_ms} ms")
    return elapsed_ms


def bench(spark, label, sql, runs=3):
    times = [run_query(spark, sql, f"{label} run{i}") for i in range(1, runs + 1)]
    avg = int(sum(times) / len(times))
    logger.info(f"  [{label}] 均值: {avg} ms  (runs: {times})")
    return avg, times


def main():
    spark = create_spark_session()
    results = {}

    logger.info("======= 开始 VARIANT Shredding 性能测试 =======")

    # ── Q1: 单 aid 精确查询（重复 10 个 aid，与上次 EMR 测试对齐）──
    logger.info("\n--- Q1: 单 aid 精确查询 ---")
    for aid in AIDS:
        q1_no = f"""
            SELECT aid,
                   COUNT(*) AS cnt,
                   SUM(variant_get(payload, '$.price', 'DOUBLE'))    AS total_price,
                   AVG(variant_get(payload, '$.score', 'DOUBLE'))    AS avg_score,
                   COUNT(DISTINCT variant_get(payload, '$.session_id', 'STRING')) AS uniq_sessions
            FROM {TABLE_NO_BUCKET}
            WHERE aid = '{aid}'
            GROUP BY aid
        """
        q1_with = q1_no.replace(TABLE_NO_BUCKET, TABLE_WITH_BUCKET)

        avg_no,   _ = bench(spark, f"Q1_no_bucket_{aid}",   q1_no)
        avg_with, _ = bench(spark, f"Q1_with_bucket_{aid}", q1_with)
        results[f"Q1_{aid}"] = {"no_bucket": avg_no, "with_bucket": avg_with}

    # ── Q2: 全表扫描聚合（各 aid 汇总）──
    logger.info("\n--- Q2: 全表扫描聚合 ---")
    q2_no = f"""
        SELECT aid,
               COUNT(*)  AS cnt,
               SUM(variant_get(payload, '$.price', 'DOUBLE'))    AS total_price,
               AVG(variant_get(payload, '$.score', 'DOUBLE'))    AS avg_score,
               COUNT(DISTINCT variant_get(payload, '$.category', 'STRING')) AS uniq_categories
        FROM {TABLE_NO_BUCKET}
        GROUP BY aid
        ORDER BY total_price DESC
    """
    q2_with = q2_no.replace(TABLE_NO_BUCKET, TABLE_WITH_BUCKET)
    avg_no,   runs_no   = bench(spark, "Q2_no_bucket",   q2_no)
    avg_with, runs_with = bench(spark, "Q2_with_bucket", q2_with)
    results["Q2_full_scan"] = {
        "no_bucket": avg_no, "no_bucket_runs": runs_no,
        "with_bucket": avg_with, "with_bucket_runs": runs_with
    }

    # ── Q3: 多 aid 过滤（5 个）──
    logger.info("\n--- Q3: 多 aid 过滤（5 个） ---")
    q3_no = f"""
        SELECT aid,
               COUNT(*) AS cnt,
               SUM(variant_get(payload, '$.quantity', 'INT'))    AS total_qty,
               AVG(variant_get(payload, '$.price', 'DOUBLE'))    AS avg_price
        FROM {TABLE_NO_BUCKET}
        WHERE aid IN ('aid_001','aid_002','aid_003','aid_004','aid_005')
        GROUP BY aid
    """
    q3_with = q3_no.replace(TABLE_NO_BUCKET, TABLE_WITH_BUCKET)
    avg_no,   runs_no   = bench(spark, "Q3_no_bucket",   q3_no)
    avg_with, runs_with = bench(spark, "Q3_with_bucket", q3_with)
    results["Q3_multi_aid"] = {
        "no_bucket": avg_no, "no_bucket_runs": runs_no,
        "with_bucket": avg_with, "with_bucket_runs": runs_with
    }

    # ── Q4: 嵌套字段聚合（geo.lat 统计）──
    logger.info("\n--- Q4: 嵌套字段聚合（geo.lat）---")
    q4_no = f"""
        SELECT aid,
               COUNT(*) AS cnt,
               AVG(variant_get(payload, '$.geo.lat', 'DOUBLE'))  AS avg_lat,
               AVG(variant_get(payload, '$.geo.lon', 'DOUBLE'))  AS avg_lon,
               COUNT(DISTINCT variant_get(payload, '$.event_type', 'STRING')) AS event_types
        FROM {TABLE_NO_BUCKET}
        WHERE aid = 'aid_001'
        GROUP BY aid
    """
    q4_with = q4_no.replace(TABLE_NO_BUCKET, TABLE_WITH_BUCKET)
    avg_no,   runs_no   = bench(spark, "Q4_no_bucket",   q4_no)
    avg_with, runs_with = bench(spark, "Q4_with_bucket", q4_with)
    results["Q4_nested"] = {
        "no_bucket": avg_no, "no_bucket_runs": runs_no,
        "with_bucket": avg_with, "with_bucket_runs": runs_with
    }

    # ── 汇总输出 ──
    logger.info("\n======= 测试结果汇总 =======")
    logger.info("PERF_RESULTS_JSON_START")
    logger.info(json.dumps(results, indent=2))
    logger.info("PERF_RESULTS_JSON_END")

    # 打印对比表（单 aid 取均值）
    q1_no_avg   = int(sum(results[f"Q1_{a}"]["no_bucket"]   for a in AIDS) / len(AIDS))
    q1_with_avg = int(sum(results[f"Q1_{a}"]["with_bucket"] for a in AIDS) / len(AIDS))

    print("\n=== VARIANT Shredding 性能对比（Spark 4.1.2）===")
    print(f"{'查询':<20} {'无分桶(ms)':>12} {'分桶(ms)':>12} {'加速比':>8} {'说明'}")
    print("-" * 70)

    rows = [
        ("Q1 单aid(均值)", q1_no_avg, q1_with_avg, "单 aid 点查，验证 bucket pruning"),
        ("Q2 全表扫描",
         results["Q2_full_scan"]["no_bucket"],
         results["Q2_full_scan"]["with_bucket"],
         "全表聚合，验证 shredding 列统计"),
        ("Q3 多aid过滤",
         results["Q3_multi_aid"]["no_bucket"],
         results["Q3_multi_aid"]["with_bucket"],
         "5 个 aid，中间场景"),
        ("Q4 嵌套字段",
         results["Q4_nested"]["no_bucket"],
         results["Q4_nested"]["with_bucket"],
         "geo.lat 嵌套字段聚合"),
    ]
    for name, no, with_, desc in rows:
        ratio = round(no / with_, 2) if with_ > 0 else "N/A"
        print(f"{name:<20} {no:>12} {with_:>12} {ratio:>8}x  {desc}")

    print(f"\nJSON_RESULTS_START:{json.dumps(results)}")
    spark.stop()
    logger.info("✅ 测试完成")


if __name__ == "__main__":
    main()

"""
最小化建表测试 — EMR Serverless (Spark 4.0) + S3TablesCatalog
验证 VARIANT 类型表创建
"""
from pyspark.sql import SparkSession

ACCOUNT_ID = "812046859005"
REGION = "us-east-1"
TABLE_BUCKET_ARN = f"arn:aws:s3tables:{REGION}:{ACCOUNT_ID}:bucket/demo-tb-bucket-812046859005"
CATALOG = "s3tablesbucket"
NAMESPACE = "variant_create_test3"

spark = (SparkSession.builder
    .appName("CreateVariantTableTest-EMR")
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
    .config("spark.sql.catalog.s3tablesbucket.warehouse", TABLE_BUCKET_ARN)
    .config("spark.sql.catalog.s3tablesbucket.client.region", REGION)
    .config("spark.sql.defaultCatalog", CATALOG)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.parquet.variantShreddingEnabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f">>> Spark version: {spark.version}")

try:
    print(">>> Step 0: CREATE NAMESPACE")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    print(">>> Namespace OK")

    print(">>> Test A: format-version=2, no VARIANT")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.test_v2_basic PURGE")
    spark.sql(f"""CREATE TABLE {CATALOG}.{NAMESPACE}.test_v2_basic (
        row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload STRING
    ) USING iceberg
    PARTITIONED BY (BUCKET(64, aid))
    TBLPROPERTIES ('format-version' = '2')""")
    print(">>> Test A PASSED")

    print(">>> Test B: format-version=3, no VARIANT")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.test_v3_no_variant PURGE")
    spark.sql(f"""CREATE TABLE {CATALOG}.{NAMESPACE}.test_v3_no_variant (
        row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload STRING
    ) USING iceberg
    PARTITIONED BY (BUCKET(64, aid))
    TBLPROPERTIES ('format-version' = '3')""")
    print(">>> Test B PASSED")

    print(">>> Test C: format-version=3, VARIANT, no partition")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.test_v3_variant_nopart PURGE")
    spark.sql(f"""CREATE TABLE {CATALOG}.{NAMESPACE}.test_v3_variant_nopart (
        row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload VARIANT
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '3')""")
    print(">>> Test C PASSED")

    print(">>> Test D: format-version=3, VARIANT + BUCKET(64, aid)")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.test_v3_variant_bucket PURGE")
    spark.sql(f"""CREATE TABLE {CATALOG}.{NAMESPACE}.test_v3_variant_bucket (
        row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload VARIANT
    ) USING iceberg
    PARTITIONED BY (BUCKET(64, aid))
    TBLPROPERTIES ('format-version' = '3')""")
    print(">>> Test D PASSED")

    print(">>> Test E: INSERT single row")
    spark.sql(f"""INSERT INTO {CATALOG}.{NAMESPACE}.test_v3_variant_nopart VALUES (
        1, 'test_aid', CAST('2024-01-01 00:00:00' AS TIMESTAMP),
        parse_json('{{"event":"test","value":42}}'))""")
    print(">>> Test E PASSED")

    print(">>> Test F: SELECT")
    spark.sql(f"SELECT * FROM {CATALOG}.{NAMESPACE}.test_v3_variant_nopart").show(truncate=False)
    print(">>> Test F PASSED")

    print(">>> ===== ALL TESTS PASSED =====")
except Exception as e:
    print(f">>> FAILED: {e}")
    import traceback
    traceback.print_exc()
    raise
finally:
    spark.stop()

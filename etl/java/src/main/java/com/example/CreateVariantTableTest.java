package com.example;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分步建表测试 — 隔离定位 S3 Tables 的 VARIANT 支持问题。
 * Test A: format-version=2, 无 VARIANT（基准连通性测试）
 * Test B: format-version=3, 无 VARIANT（验证 V3 支持）
 * Test C: format-version=3, 有 VARIANT（目标测试）
 */
public class CreateVariantTableTest {

    private static final Logger log = LoggerFactory.getLogger(CreateVariantTableTest.class);
    private static final String CATALOG = "s3tablesbucket";
    private static final String NAMESPACE = "variant_create_test2";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CreateVariantTableTest")
                .getOrCreate();
        // 使用 ERROR 级别以外的自定义打印
        spark.sparkContext().setLogLevel("WARN");

        System.out.println(">>> Spark version: " + spark.version());
        System.out.println(">>> Catalog impl: " + spark.conf().get("spark.sql.catalog.s3tablesbucket.catalog-impl"));

        try {
            // Create namespace
            System.out.println(">>> Step 0: CREATE NAMESPACE");
            spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + "." + NAMESPACE);
            System.out.println(">>> Namespace OK");

            // Test A: format-version=2, no VARIANT
            System.out.println(">>> Test A: format-version=2, no VARIANT");
            spark.sql("DROP TABLE IF EXISTS " + CATALOG + "." + NAMESPACE + ".test_v2_basic PURGE");
            spark.sql("CREATE TABLE " + CATALOG + "." + NAMESPACE + ".test_v2_basic (\n" +
                    "    row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload STRING\n" +
                    ") USING iceberg\n" +
                    "PARTITIONED BY (BUCKET(64, aid))\n" +
                    "TBLPROPERTIES ('format-version' = '2')");
            System.out.println(">>> Test A PASSED: V2 basic table created");

            // Test B: format-version=3, no VARIANT
            System.out.println(">>> Test B: format-version=3, no VARIANT");
            spark.sql("DROP TABLE IF EXISTS " + CATALOG + "." + NAMESPACE + ".test_v3_no_variant PURGE");
            spark.sql("CREATE TABLE " + CATALOG + "." + NAMESPACE + ".test_v3_no_variant (\n" +
                    "    row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload STRING\n" +
                    ") USING iceberg\n" +
                    "PARTITIONED BY (BUCKET(64, aid))\n" +
                    "TBLPROPERTIES ('format-version' = '3')");
            System.out.println(">>> Test B PASSED: V3 table (no variant) created");

            // Test C: format-version=3, with VARIANT, no partition
            System.out.println(">>> Test C: format-version=3, VARIANT, no partition");
            spark.sql("DROP TABLE IF EXISTS " + CATALOG + "." + NAMESPACE + ".test_v3_variant_nopart PURGE");
            spark.sql("CREATE TABLE " + CATALOG + "." + NAMESPACE + ".test_v3_variant_nopart (\n" +
                    "    row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload VARIANT\n" +
                    ") USING iceberg\n" +
                    "TBLPROPERTIES ('format-version' = '3')");
            System.out.println(">>> Test C PASSED");

            // Test D: format-version=3, with VARIANT + BUCKET partition
            System.out.println(">>> Test D: format-version=3, VARIANT + BUCKET(64, aid)");
            spark.sql("DROP TABLE IF EXISTS " + CATALOG + "." + NAMESPACE + ".test_v3_variant_bucket PURGE");
            spark.sql("CREATE TABLE " + CATALOG + "." + NAMESPACE + ".test_v3_variant_bucket (\n" +
                    "    row_id BIGINT, aid STRING, event_ts TIMESTAMP, payload VARIANT\n" +
                    ") USING iceberg\n" +
                    "PARTITIONED BY (BUCKET(64, aid))\n" +
                    "TBLPROPERTIES ('format-version' = '3')");
            System.out.println(">>> Test D PASSED");

            // Test E: Insert into variant table
            System.out.println(">>> Test E: INSERT single row");
            spark.sql("INSERT INTO " + CATALOG + "." + NAMESPACE + ".test_v3_variant_nopart VALUES (" +
                    "1, 'test_aid', CAST('2024-01-01 00:00:00' AS TIMESTAMP), " +
                    "parse_json('{\"event\":\"test\",\"value\":42}'))");
            System.out.println(">>> Test E PASSED");

            // Test F: Read back
            System.out.println(">>> Test F: SELECT");
            spark.sql("SELECT * FROM " + CATALOG + "." + NAMESPACE + ".test_v3_variant_nopart").show(false);
            System.out.println(">>> Test F PASSED");

            System.out.println(">>> ===== ALL TESTS PASSED =====");
        } catch (Exception e) {
            System.out.println(">>> FAILED: " + e.getMessage());
            e.printStackTrace(System.out);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}

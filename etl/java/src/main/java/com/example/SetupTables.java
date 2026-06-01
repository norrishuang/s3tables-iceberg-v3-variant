package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 建表 + 数据生成（Variant Shredding 对比实验）
 *
 * 表设计：
 *   variant_no_bucket    — VARIANT payload，无分桶，format-version=3
 *   variant_with_bucket  — VARIANT payload，BUCKET(64,aid) 分桶，format-version=3
 *
 * 数据：模拟电商/游戏行为日志，1000 万行
 * Catalog 通过 sparkConf 传入（REST Catalog → demo-tb-bucket-812046859005）
 */
public class SetupTables {

    private static final Logger log = LoggerFactory.getLogger(SetupTables.class);

    private static final String CATALOG = "s3tablesbucket";
    private static final String NAMESPACE = "variant_shredding_test";
    private static final String TABLE_NO_BUCKET = NAMESPACE + ".variant_no_bucket";
    private static final String TABLE_WITH_BUCKET = NAMESPACE + ".variant_with_bucket";
    private static final long TARGET_ROWS = 10_000_000L;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Variant-Shredding-Tables-Setup")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark 版本: {}", spark.version());

        try {
            createTables(spark);
            generateAndWrite(spark);
            verify(spark);
            log.info("===== 建表完成 =====");
        } catch (Exception e) {
            log.error("失败: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    static void createTables(SparkSession spark) {
        log.info("=== 创建 Namespace ===");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + "." + NAMESPACE);

        log.info("=== 删除旧表（如存在）===");
        spark.sql("DROP TABLE IF EXISTS " + TABLE_NO_BUCKET + " PURGE");
        spark.sql("DROP TABLE IF EXISTS " + TABLE_WITH_BUCKET + " PURGE");

        log.info("=== 创建 variant_no_bucket ===");
        spark.sql("CREATE TABLE " + TABLE_NO_BUCKET + " (\n" +
                "    row_id   BIGINT,\n" +
                "    aid      STRING,\n" +
                "    event_ts TIMESTAMP,\n" +
                "    payload  VARIANT\n" +
                ") USING iceberg\n" +
                "TBLPROPERTIES (\n" +
                "    'format-version' = '3',\n" +
                "    'write.format.default' = 'parquet',\n" +
                "    'write.parquet.compression-codec' = 'snappy'\n" +
                ")");

        log.info("=== 创建 variant_with_bucket ===");
        spark.sql("CREATE TABLE " + TABLE_WITH_BUCKET + " (\n" +
                "    row_id   BIGINT,\n" +
                "    aid      STRING,\n" +
                "    event_ts TIMESTAMP,\n" +
                "    payload  VARIANT\n" +
                ") USING iceberg\n" +
                "PARTITIONED BY (BUCKET(64, aid))\n" +
                "TBLPROPERTIES (\n" +
                "    'format-version' = '3',\n" +
                "    'write.format.default' = 'parquet',\n" +
                "    'write.parquet.compression-codec' = 'snappy'\n" +
                ")");
        log.info("两张表创建完成");
    }

    static void generateAndWrite(SparkSession spark) throws Exception {
        log.info("=== 生成 {} 行数据 ===", String.format("%,d", TARGET_ROWS));

        // 用 Spark SQL 生成数据，避免 Java 循环
        spark.sql("SELECT id FROM range(" + TARGET_ROWS + ")").createOrReplaceTempView("ids");

        Dataset<Row> df = spark.sql(
                "SELECT \n" +
                "    id AS row_id,\n" +
                "    element_at(array('aid_001','aid_002','aid_003','aid_004','aid_005',\n" +
                "                     'aid_006','aid_007','aid_008','aid_009','aid_010'),\n" +
                "              CAST(id % 10 + 1 AS INT)) AS aid,\n" +
                "    CAST(1704067200 + rand(42) * 31536000 AS TIMESTAMP) AS event_ts,\n" +
                "    parse_json(\n" +
                "        to_json(named_struct(\n" +
                "            'event_type', element_at(array('click','purchase','view','search','add_cart','checkout'), CAST(id % 6 + 1 AS INT)),\n" +
                "            'price', CAST(rand(1) * 999 + 1 AS DECIMAL(10,2)),\n" +
                "            'quantity', CAST(rand(2) * 10 + 1 AS INT),\n" +
                "            'category', element_at(array('electronics','clothing','food','sports','books'), CAST(id % 5 + 1 AS INT)),\n" +
                "            'score', CAST(rand(3) * 100 AS DECIMAL(5,2)),\n" +
                "            'session_id', concat('sess_', CAST(id AS STRING)),\n" +
                "            'geo', named_struct('lat', CAST(rand(4) * 180 - 90 AS DECIMAL(8,5)), 'lon', CAST(rand(5) * 360 - 180 AS DECIMAL(8,5)))\n" +
                "        ))\n" +
                "    ) AS payload\n" +
                "FROM ids"
        );

        log.info("写入 variant_no_bucket ...");
        df.writeTo(TABLE_NO_BUCKET).append();
        long cnt1 = spark.sql("SELECT COUNT(*) FROM " + TABLE_NO_BUCKET).collectAsList().get(0).getLong(0);
        log.info("variant_no_bucket: {} 行", String.format("%,d", cnt1));

        log.info("写入 variant_with_bucket ...");
        df.writeTo(TABLE_WITH_BUCKET).append();
        long cnt2 = spark.sql("SELECT COUNT(*) FROM " + TABLE_WITH_BUCKET).collectAsList().get(0).getLong(0);
        log.info("variant_with_bucket: {} 行", String.format("%,d", cnt2));
    }

    static void verify(SparkSession spark) {
        log.info("=== 验证数据 ===");
        for (String tbl : new String[]{TABLE_NO_BUCKET, TABLE_WITH_BUCKET}) {
            spark.sql(
                    "SELECT aid, COUNT(*) AS cnt, " +
                    "AVG(variant_get(payload, '$.price', 'DOUBLE')) AS avg_price " +
                    "FROM " + tbl + " GROUP BY aid ORDER BY aid"
            ).show();
        }
    }
}

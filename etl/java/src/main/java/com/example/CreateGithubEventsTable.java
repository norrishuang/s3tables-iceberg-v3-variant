package com.example;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建 GitHub Events 对比测试表 (s3tables catalog)
 *
 * 两张表:
 *   github.events_json    - object 字段存为 STRING (JSON)
 *   github.events_variant - object 字段存为 VARIANT (shredding)
 *
 * Object 字段: actor, repo, payload, org
 * 固定字段: id, type, public, created_at
 */
public class CreateGithubEventsTable {

    private static final Logger log = LoggerFactory.getLogger(CreateGithubEventsTable.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Create-GitHub-Events-Tables")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark: {}", spark.version());

        // 创建 namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS s3t.github");

        // === JSON 表: object 字段存为 STRING ===
        String jsonTable = """
            CREATE TABLE IF NOT EXISTS s3t.github.events_json (
                id              STRING      COMMENT '事件ID',
                type            STRING      COMMENT '事件类型',
                public          BOOLEAN     COMMENT '是否公开',
                created_at      TIMESTAMP   COMMENT '事件时间',
                actor           STRING      COMMENT 'Actor对象 (JSON string)',
                repo            STRING      COMMENT 'Repo对象 (JSON string)',
                payload         STRING      COMMENT 'Payload对象 (JSON string)',
                org             STRING      COMMENT 'Org对象 (JSON string, 可空)'
            )
            USING iceberg
            PARTITIONED BY (days(created_at), type)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.parquet.compression-codec' = 'zstd'
            )
            """;
        log.info("Creating s3t.github.events_json ...");
        spark.sql(jsonTable);
        log.info("events_json created.");

        // === Variant 表: object 字段存为 VARIANT ===
        String variantTable = """
            CREATE TABLE IF NOT EXISTS s3t.github.events_variant (
                id              STRING      COMMENT '事件ID',
                type            STRING      COMMENT '事件类型',
                public          BOOLEAN     COMMENT '是否公开',
                created_at      TIMESTAMP   COMMENT '事件时间',
                actor           VARIANT     COMMENT 'Actor对象 (variant shredding)',
                repo            VARIANT     COMMENT 'Repo对象 (variant shredding)',
                payload         VARIANT     COMMENT 'Payload对象 (variant shredding)',
                org             VARIANT     COMMENT 'Org对象 (variant shredding, 可空)'
            )
            USING iceberg
            PARTITIONED BY (days(created_at), type)
            TBLPROPERTIES (
                'format-version' = '3',
                'write.parquet.compression-codec' = 'zstd',
                'write.parquet.shred-variants' = 'true'
            )
            """;
        log.info("Creating s3t.github.events_variant ...");
        spark.sql(variantTable);
        log.info("events_variant created.");

        // 验证
        log.info("=== events_json schema ===");
        spark.sql("DESCRIBE EXTENDED s3t.github.events_json").show(30, false);
        log.info("=== events_variant schema ===");
        spark.sql("DESCRIBE EXTENDED s3t.github.events_variant").show(30, false);

        spark.stop();
    }
}

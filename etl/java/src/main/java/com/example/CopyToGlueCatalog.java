package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * 复制 s3t.github.events_variant → Glue catalog Iceberg 表 (glue.github.events_variant)
 * 目的: 在 S3 上查看 Iceberg 文件目录结构
 */
public class CopyToGlueCatalog {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Copy-To-Glue-Catalog")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        System.out.println("Spark: " + spark.version());

        // 创建 Glue database
        spark.sql("CREATE DATABASE IF NOT EXISTS glue.github");

        // 创建目标表 (同 schema)
        String createSql = """
            CREATE TABLE IF NOT EXISTS glue.github.events_variant (
                id              STRING,
                type            STRING,
                `public`        BOOLEAN,
                created_at      TIMESTAMP,
                actor           VARIANT,
                repo            VARIANT,
                payload         VARIANT,
                org             VARIANT
            )
            USING iceberg
            PARTITIONED BY (days(created_at), type)
            TBLPROPERTIES (
                'format-version' = '3',
                'write.parquet.compression-codec' = 'zstd',
                'write.parquet.shred-variants' = 'true'
            )
            """;
        System.out.println("Creating glue.github.events_variant ...");
        spark.sql(createSql);
        System.out.println("Table created.");

        // 复制数据
        System.out.println("Copying data from s3t.github.events_variant ...");
        Dataset<Row> df = spark.sql("SELECT * FROM s3t.github.events_variant");
        long count = df.count();
        System.out.println("Source rows: " + String.format("%,d", count));

        try {
            df.writeTo("glue.github.events_variant").append();
        } catch (Exception e) {
            throw new RuntimeException("写入失败", e);
        }

        long written = spark.sql("SELECT COUNT(*) FROM glue.github.events_variant").collectAsList().get(0).getLong(0);
        System.out.println("Done! Target rows: " + String.format("%,d", written));
        spark.stop();
    }
}

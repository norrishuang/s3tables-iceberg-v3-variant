package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * 性能对比测试：JSON 表 vs Variant Shredding 表
 *
 * 4 张表：
 *   srccat.gamedb.game_action_logs           — JSON (STRING detail), 无 bucket
 *   srccat.gamedb.game_action_logs_bucket    — JSON (STRING detail), bucket(64,aid)
 *   dstcat.iceberg_v3_test.variant_test_no_partition — VARIANT detail, hours(tm)
 *   dstcat.iceberg_v3_test.variant_test_with_bucket  — VARIANT detail, hours(tm)+bucket(64,aid)
 *
 * 每张表串行测试 10 个 aid，输出每次查询耗时。
 * 结果通过 System.out.println 输出（避免 SLF4J NOP logger 问题）。
 */
public class FullPerfTest {

    static final String[] TABLES = {
        "srccat.gamedb.game_action_logs",
        "srccat.gamedb.game_action_logs_bucket",
        "dstcat.iceberg_v3_test.variant_test_no_partition",
        "dstcat.iceberg_v3_test.variant_test_with_bucket"
    };

    static final String[] AIDS = {
        "5994833300571649523", "7628759510761459518", "4484309606637386521",
        "5817864250259133818", "1690225376489299568", "499510731539886708",
        "3404392457017579511", "6870130936608314155", "160514536417143311",
        "2816998923098210381"
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Full-PerfTest-JSON-vs-Variant")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== 性能对比测试开始 ===");
        System.out.println("Spark 版本: " + spark.version());

        // 1. 收集表元数据
        System.out.println("\n=== 表元数据 ===");
        for (String table : TABLES) {
            try {
                long count = spark.sql("SELECT COUNT(*) FROM " + table).collectAsList().get(0).getLong(0);
                System.out.println("TABLE_META|" + table + "|rows=" + count);
            } catch (Exception e) {
                System.out.println("TABLE_META|" + table + "|ERROR=" + e.getMessage());
            }
        }

        // 2. 获取表快照信息（文件数、大小）
        System.out.println("\n=== 表存储信息 ===");
        for (String table : TABLES) {
            try {
                Dataset<Row> files = spark.sql(
                    "SELECT COUNT(*) as file_count, SUM(file_size_in_bytes) as total_bytes " +
                    "FROM " + table + ".files"
                );
                Row r = files.collectAsList().get(0);
                System.out.println("TABLE_STORAGE|" + table + "|files=" + r.getLong(0) + "|bytes=" + r.getLong(1));
            } catch (Exception e) {
                System.out.println("TABLE_STORAGE|" + table + "|ERROR=" + e.getMessage());
            }
        }

        // 3. 性能测试：每张表 × 10 个 aid
        System.out.println("\n=== 查询性能测试 ===");
        for (String table : TABLES) {
            System.out.println("\n--- " + table + " ---");
            boolean isVariant = table.contains("variant");

            for (String aid : AIDS) {
                String sql;
                if (isVariant) {
                    // Variant 表：detail 是 VARIANT 类型，用 variant_get 提取字段
                    sql = "SELECT aid, COUNT(*) AS cnt, " +
                          "SUM(CAST(variant_get(detail, '$.gold') AS BIGINT)) AS total_gold " +
                          "FROM " + table + " WHERE aid = " + aid + " GROUP BY aid";
                } else {
                    // JSON 表：detail 是 STRING 类型，用 get_json_object 提取字段
                    sql = "SELECT aid, COUNT(*) AS cnt, " +
                          "SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold " +
                          "FROM " + table + " WHERE aid = " + aid + " GROUP BY aid";
                }

                long start = System.currentTimeMillis();
                try {
                    spark.sql(sql).collect();
                } catch (Exception e) {
                    System.out.println("QUERY_ERROR|" + table + "|aid=" + aid + "|error=" + e.getMessage());
                    continue;
                }
                long elapsed = System.currentTimeMillis() - start;
                System.out.println("QUERY_RESULT|" + table + "|aid=" + aid + "|ms=" + elapsed);
            }
        }

        System.out.println("\n=== 性能对比测试完成 ===");
        spark.stop();
    }
}

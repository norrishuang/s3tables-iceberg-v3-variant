package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * Variant vs JSON 性能对比测试
 *
 * 对比两张含 bucket(64,aid) 的表，验证 Agent/分析师常见查询场景下
 * Variant(Shredding) 相比 JSON STRING 的性能优势。
 *
 * JSON 表:    srccat.gamedb.game_action_logs_bucket
 *             字段: log_id, aid(BIGINT), event, uin, tm(TIMESTAMP), detail(STRING)
 * Variant 表: dstcat.iceberg_v3_test.variant_test_with_bucket
 *             字段: event_id, aid(BIGINT), tm(TIMESTAMP), detail(VARIANT)
 *
 * detail 中的嵌套字段: gold, player_start_time, map_id, cloud_area, map_type, session_id
 *
 * 测试场景:
 *   Q1: 查某玩家在某地图的记录 (aid + map_id)
 *   Q2: 查某时间段某地图类型的所有会话 (tm范围 + map_type)
 *   Q3: 统计每张地图的玩家数 (全表聚合)
 *   Q4: 查某cloud_area中游戏时长超1小时的玩家 (嵌套字段计算)
 *   Q5: 多字段同时提取 (gold, map_id, session_id, cloud_area)
 *   Q6: 按cloud_area统计平均gold (嵌套条件聚合)
 */
public class VariantVsJsonPerfTest {

    static final String JSON_TABLE = "srccat.gamedb.game_action_logs_bucket";
    static final String VARIANT_TABLE = "dstcat.iceberg_v3_test.variant_test_with_bucket";
    static final int RUNS = 3;

    // 已知存在的 aid
    static final String AID = "5994833300571649523";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Variant-vs-JSON-PerfTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Variant vs JSON 嵌套字段查询性能对比 ===");
        System.out.println("Spark: " + spark.version());
        System.out.println("JSON表: " + JSON_TABLE);
        System.out.println("Variant表: " + VARIANT_TABLE);
        System.out.println("每查询运行 " + RUNS + " 次取均值");
        System.out.println();

        // 表元数据
        System.out.println("=== 表元数据 ===");
        printMeta(spark, JSON_TABLE);
        printMeta(spark, VARIANT_TABLE);

        // Q1: 查某玩家在某地图的记录
        System.out.println("\n=== Q1: 查某玩家在某地图的记录 (aid + map_id过滤) ===");
        String q1Json = "SELECT aid, tm, " +
                "get_json_object(detail, '$.map_id') AS map_id, " +
                "get_json_object(detail, '$.gold') AS gold, " +
                "get_json_object(detail, '$.session_id') AS session_id " +
                "FROM " + JSON_TABLE + " " +
                "WHERE aid = " + AID + " AND get_json_object(detail, '$.map_id') = '1001' " +
                "LIMIT 100";
        String q1Var = "SELECT aid, tm, " +
                "variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "variant_get(detail, '$.gold', 'LONG') AS gold, " +
                "variant_get(detail, '$.session_id', 'STRING') AS session_id " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE aid = " + AID + " AND variant_get(detail, '$.map_id', 'STRING') = '1001' " +
                "LIMIT 100";
        long q1J = bench(spark, q1Json, "Q1_JSON");
        long q1V = bench(spark, q1Var, "Q1_VARIANT");

        // Q2: 查某时间段某地图类型的所有会话
        System.out.println("\n=== Q2: 查某时间段某地图类型的所有会话 (tm + map_type) ===");
        String q2Json = "SELECT aid, tm, " +
                "get_json_object(detail, '$.session_id') AS session_id, " +
                "get_json_object(detail, '$.map_type') AS map_type " +
                "FROM " + JSON_TABLE + " " +
                "WHERE tm BETWEEN '2026-05-01' AND '2026-05-02' " +
                "AND get_json_object(detail, '$.map_type') = '5' " +
                "LIMIT 200";
        String q2Var = "SELECT aid, tm, " +
                "variant_get(detail, '$.session_id', 'STRING') AS session_id, " +
                "variant_get(detail, '$.map_type', 'STRING') AS map_type " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE tm BETWEEN '2026-05-01' AND '2026-05-02' " +
                "AND variant_get(detail, '$.map_type', 'STRING') = '5' " +
                "LIMIT 200";
        long q2J = bench(spark, q2Json, "Q2_JSON");
        long q2V = bench(spark, q2Var, "Q2_VARIANT");

        // Q3: 统计每张地图的玩家数
        System.out.println("\n=== Q3: 统计每张地图的玩家数 (全表聚合) ===");
        String q3Json = "SELECT get_json_object(detail, '$.map_id') AS map_id, " +
                "COUNT(DISTINCT aid) AS player_count, " +
                "COUNT(*) AS record_count " +
                "FROM " + JSON_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "GROUP BY get_json_object(detail, '$.map_id') " +
                "ORDER BY record_count DESC";
        String q3Var = "SELECT variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "COUNT(DISTINCT aid) AS player_count, " +
                "COUNT(*) AS record_count " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "GROUP BY variant_get(detail, '$.map_id', 'STRING') " +
                "ORDER BY record_count DESC";
        long q3J = bench(spark, q3Json, "Q3_JSON");
        long q3V = bench(spark, q3Var, "Q3_VARIANT");

        // Q4: 查某cloud_area中游戏时长超过1小时的玩家
        System.out.println("\n=== Q4: 查某cloud_area中游戏时长超1小时的玩家 ===");
        String q4Json = "SELECT aid, tm, " +
                "get_json_object(detail, '$.cloud_area') AS cloud_area, " +
                "get_json_object(detail, '$.player_start_time') AS start_time, " +
                "(unix_timestamp(tm) - CAST(get_json_object(detail, '$.player_start_time') AS BIGINT)) AS duration_sec " +
                "FROM " + JSON_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "AND get_json_object(detail, '$.cloud_area') IS NOT NULL " +
                "AND CAST(get_json_object(detail, '$.player_start_time') AS BIGINT) > 0 " +
                "AND (unix_timestamp(tm) - CAST(get_json_object(detail, '$.player_start_time') AS BIGINT)) > 3600 " +
                "LIMIT 100";
        String q4Var = "SELECT aid, tm, " +
                "variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area, " +
                "variant_get(detail, '$.player_start_time', 'LONG') AS start_time, " +
                "(unix_timestamp(tm) - CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT)) AS duration_sec " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "AND variant_get(detail, '$.cloud_area', 'STRING') IS NOT NULL " +
                "AND CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT) > 0 " +
                "AND (unix_timestamp(tm) - CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT)) > 3600 " +
                "LIMIT 100";
        long q4J = bench(spark, q4Json, "Q4_JSON");
        long q4V = bench(spark, q4Var, "Q4_VARIANT");

        // Q5: 多字段同时提取
        System.out.println("\n=== Q5: 多字段同时提取 (gold, map_id, session_id, cloud_area) ===");
        String q5Json = "SELECT aid, tm, " +
                "get_json_object(detail, '$.gold') AS gold, " +
                "get_json_object(detail, '$.map_id') AS map_id, " +
                "get_json_object(detail, '$.session_id') AS session_id, " +
                "get_json_object(detail, '$.cloud_area') AS cloud_area " +
                "FROM " + JSON_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "LIMIT 500";
        String q5Var = "SELECT aid, tm, " +
                "variant_get(detail, '$.gold', 'LONG') AS gold, " +
                "variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "variant_get(detail, '$.session_id', 'STRING') AS session_id, " +
                "variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "LIMIT 500";
        long q5J = bench(spark, q5Json, "Q5_JSON");
        long q5V = bench(spark, q5Var, "Q5_VARIANT");

        // Q6: 按cloud_area统计平均gold
        System.out.println("\n=== Q6: 按cloud_area统计平均gold (嵌套条件聚合) ===");
        String q6Json = "SELECT " +
                "get_json_object(detail, '$.cloud_area') AS cloud_area, " +
                "AVG(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS avg_gold, " +
                "COUNT(*) AS cnt " +
                "FROM " + JSON_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "AND get_json_object(detail, '$.cloud_area') IS NOT NULL " +
                "GROUP BY get_json_object(detail, '$.cloud_area') " +
                "ORDER BY avg_gold DESC";
        String q6Var = "SELECT " +
                "variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area, " +
                "AVG(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS avg_gold, " +
                "COUNT(*) AS cnt " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE aid = " + AID + " " +
                "AND variant_get(detail, '$.cloud_area', 'STRING') IS NOT NULL " +
                "GROUP BY variant_get(detail, '$.cloud_area', 'STRING') " +
                "ORDER BY avg_gold DESC";
        long q6J = bench(spark, q6Json, "Q6_JSON");
        long q6V = bench(spark, q6Var, "Q6_VARIANT");

        // 汇总
        System.out.println("\n=== 性能对比汇总 ===");
        System.out.println("SUMMARY_HEADER|Query|JSON(ms)|Variant(ms)|Speedup");
        printSummary("Q1_aid+map_id", q1J, q1V);
        printSummary("Q2_tm+map_type", q2J, q2V);
        printSummary("Q3_map_agg", q3J, q3V);
        printSummary("Q4_duration_calc", q4J, q4V);
        printSummary("Q5_multi_field", q5J, q5V);
        printSummary("Q6_nested_agg", q6J, q6V);

        System.out.println("\n=== 测试完成 ===");
        spark.stop();
    }

    static long bench(SparkSession spark, String sql, String label) {
        long total = 0;
        for (int i = 1; i <= RUNS; i++) {
            long start = System.currentTimeMillis();
            try {
                spark.sql(sql).collect();
            } catch (Exception e) {
                System.out.println("ERROR|" + label + "|run" + i + "|" + e.getMessage());
                return -1;
            }
            long elapsed = System.currentTimeMillis() - start;
            System.out.println("RUN|" + label + "|run" + i + "|" + elapsed + "ms");
            total += elapsed;
        }
        long avg = total / RUNS;
        System.out.println("AVG|" + label + "|" + avg + "ms");
        return avg;
    }

    static void printMeta(SparkSession spark, String table) {
        try {
            long count = spark.sql("SELECT COUNT(*) FROM " + table).collectAsList().get(0).getLong(0);
            System.out.println("META|" + table + "|rows=" + count);
        } catch (Exception e) {
            System.out.println("META|" + table + "|ERROR=" + e.getMessage());
        }
        try {
            Row r = spark.sql("SELECT COUNT(*) as fc, SUM(file_size_in_bytes) as tb FROM " + table + ".files")
                    .collectAsList().get(0);
            System.out.println("STORAGE|" + table + "|files=" + r.getLong(0) + "|bytes=" + r.getLong(1));
        } catch (Exception e) {
            System.out.println("STORAGE|" + table + "|ERROR=" + e.getMessage());
        }
    }

    static void printSummary(String query, long jsonMs, long variantMs) {
        String speedup = (jsonMs > 0 && variantMs > 0)
                ? String.format("%.2fx", (double) jsonMs / variantMs)
                : "N/A";
        System.out.println("SUMMARY|" + query + "|" + jsonMs + "|" + variantMs + "|" + speedup);
    }
}

package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * 性能对比: game_action_logs_8b_json vs game_action_logs_8b_variant
 * 两张表同 catalog、同字段结构、同分区(hours(tm)+bucket(8,aid))，仅 detail 类型不同。
 *
 * 字段: aid, event, uin, roomid, lv, mode, version, area, tm, log_id, detail
 * detail 嵌套字段: gold, player_start_time, map_id, cloud_area, map_type, session_id, map_name
 */
public class PerfTest8b {

    static final String JSON_TABLE = "srccat.gamedb.game_action_logs_8b_json";
    static final String VARIANT_TABLE = "srccat.gamedb.game_action_logs_8b_variant";
    static final int RUNS = 3;
    static final String AID = "5994833300571649523";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("PerfTest-8b-JSON-vs-Variant")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== 8b 表性能对比测试 ===");
        System.out.println("JSON:    " + JSON_TABLE);
        System.out.println("Variant: " + VARIANT_TABLE);
        System.out.println("每查询 " + RUNS + " 次取均值\n");

        // 表元数据
        printMeta(spark, JSON_TABLE);
        printMeta(spark, VARIANT_TABLE);

        // Q1: 单玩家 + 嵌套字段过滤
        System.out.println("\n=== Q1: 单玩家 + map_id过滤 ===");
        long q1J = bench(spark, "SELECT aid, tm, " +
                "get_json_object(detail, '$.map_id') AS map_id, " +
                "get_json_object(detail, '$.gold') AS gold " +
                "FROM " + JSON_TABLE + " WHERE aid = " + AID +
                " AND get_json_object(detail, '$.map_id') = '1001' LIMIT 100", "Q1_JSON");
        long q1V = bench(spark, "SELECT aid, tm, " +
                "variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "variant_get(detail, '$.gold', 'LONG') AS gold " +
                "FROM " + VARIANT_TABLE + " WHERE aid = " + AID +
                " AND variant_get(detail, '$.map_id', 'STRING') = '1001' LIMIT 100", "Q1_VAR");

        // Q2: 时间范围 + 嵌套字段过滤
        System.out.println("\n=== Q2: 时间范围 + map_type过滤 ===");
        long q2J = bench(spark, "SELECT aid, tm, " +
                "get_json_object(detail, '$.session_id') AS session_id " +
                "FROM " + JSON_TABLE +
                " WHERE tm BETWEEN '2026-05-01' AND '2026-05-02'" +
                " AND get_json_object(detail, '$.map_type') = '5' LIMIT 200", "Q2_JSON");
        long q2V = bench(spark, "SELECT aid, tm, " +
                "variant_get(detail, '$.session_id', 'STRING') AS session_id " +
                "FROM " + VARIANT_TABLE +
                " WHERE tm BETWEEN '2026-05-01' AND '2026-05-02'" +
                " AND variant_get(detail, '$.map_type', 'STRING') = '5' LIMIT 200", "Q2_VAR");

        // Q3: 单玩家聚合 (按 map_id 分组)
        System.out.println("\n=== Q3: 单玩家按map_id聚合 ===");
        long q3J = bench(spark, "SELECT get_json_object(detail, '$.map_id') AS map_id, " +
                "COUNT(*) AS cnt, " +
                "SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold " +
                "FROM " + JSON_TABLE + " WHERE aid = " + AID +
                " GROUP BY get_json_object(detail, '$.map_id') ORDER BY cnt DESC", "Q3_JSON");
        long q3V = bench(spark, "SELECT variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "COUNT(*) AS cnt, " +
                "SUM(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS total_gold " +
                "FROM " + VARIANT_TABLE + " WHERE aid = " + AID +
                " GROUP BY variant_get(detail, '$.map_id', 'STRING') ORDER BY cnt DESC", "Q3_VAR");

        // Q4: 多字段提取
        System.out.println("\n=== Q4: 多字段提取 (gold, map_id, session_id, cloud_area, map_name) ===");
        long q4J = bench(spark, "SELECT aid, tm, " +
                "get_json_object(detail, '$.gold') AS gold, " +
                "get_json_object(detail, '$.map_id') AS map_id, " +
                "get_json_object(detail, '$.session_id') AS session_id, " +
                "get_json_object(detail, '$.cloud_area') AS cloud_area, " +
                "get_json_object(detail, '$.map_name') AS map_name " +
                "FROM " + JSON_TABLE + " WHERE aid = " + AID + " LIMIT 500", "Q4_JSON");
        long q4V = bench(spark, "SELECT aid, tm, " +
                "variant_get(detail, '$.gold', 'LONG') AS gold, " +
                "variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "variant_get(detail, '$.session_id', 'STRING') AS session_id, " +
                "variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area, " +
                "variant_get(detail, '$.map_name', 'STRING') AS map_name " +
                "FROM " + VARIANT_TABLE + " WHERE aid = " + AID + " LIMIT 500", "Q4_VAR");

        // Q5: 嵌套条件聚合 (按 cloud_area 统计平均 gold)
        System.out.println("\n=== Q5: 按cloud_area统计平均gold ===");
        long q5J = bench(spark, "SELECT " +
                "get_json_object(detail, '$.cloud_area') AS cloud_area, " +
                "AVG(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS avg_gold, " +
                "COUNT(*) AS cnt " +
                "FROM " + JSON_TABLE + " WHERE aid = " + AID +
                " AND get_json_object(detail, '$.cloud_area') IS NOT NULL" +
                " GROUP BY get_json_object(detail, '$.cloud_area') ORDER BY avg_gold DESC", "Q5_JSON");
        long q5V = bench(spark, "SELECT " +
                "variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area, " +
                "AVG(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS avg_gold, " +
                "COUNT(*) AS cnt " +
                "FROM " + VARIANT_TABLE + " WHERE aid = " + AID +
                " AND variant_get(detail, '$.cloud_area', 'STRING') IS NOT NULL" +
                " GROUP BY variant_get(detail, '$.cloud_area', 'STRING') ORDER BY avg_gold DESC", "Q5_VAR");

        // Q6: 大范围扫描聚合 (1天数据, 按 map_id 聚合)
        System.out.println("\n=== Q6: 1天数据按map_id聚合 ===");
        long q6J = bench(spark, "SELECT get_json_object(detail, '$.map_id') AS map_id, " +
                "COUNT(*) AS cnt, COUNT(DISTINCT aid) AS players " +
                "FROM " + JSON_TABLE +
                " WHERE CAST(tm AS DATE) = '2026-05-15'" +
                " GROUP BY get_json_object(detail, '$.map_id') ORDER BY cnt DESC LIMIT 20", "Q6_JSON");
        long q6V = bench(spark, "SELECT variant_get(detail, '$.map_id', 'STRING') AS map_id, " +
                "COUNT(*) AS cnt, COUNT(DISTINCT aid) AS players " +
                "FROM " + VARIANT_TABLE +
                " WHERE CAST(tm AS DATE) = '2026-05-15'" +
                " GROUP BY variant_get(detail, '$.map_id', 'STRING') ORDER BY cnt DESC LIMIT 20", "Q6_VAR");

        // 汇总
        System.out.println("\n=== 性能对比汇总 ===");
        System.out.println("SUMMARY_HEADER|Query|JSON(ms)|Variant(ms)|Speedup");
        printSummary("Q1_aid+map_id_filter", q1J, q1V);
        printSummary("Q2_time+map_type", q2J, q2V);
        printSummary("Q3_aid_group_by", q3J, q3V);
        printSummary("Q4_multi_field_extract", q4J, q4V);
        printSummary("Q5_nested_agg", q5J, q5V);
        printSummary("Q6_daily_scan_agg", q6J, q6V);

        System.out.println("\n=== 测试完成 ===");
        spark.stop();
    }

    static long bench(SparkSession spark, String sql, String label) {
        // warmup
        try { spark.sql(sql).collect(); } catch (Exception e) { /* ignore */ }

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
                ? String.format("%.2fx", (double) jsonMs / variantMs) : "N/A";
        System.out.println("SUMMARY|" + query + "|" + jsonMs + "|" + variantMs + "|" + speedup);
    }
}

package com.example;

import org.apache.spark.sql.*;

/**
 * 性能对比测试 Round 2：复杂 Self-Join 查询
 *
 * JSON 源表有完整列 (aid, event, uin, tm, detail)，可执行 self-join。
 * Variant 表只有 (event_id, aid, tm, detail)，uin/event 不在 detail 中，
 * 因此 Variant 表无法执行等价 self-join。
 *
 * 本测试对 4 张表统一执行相同逻辑的查询：
 * - JSON 表：用 get_json_object 从 detail 提取 player_start_time，用独立列 event/uin 做 join
 * - Variant 表：由于缺少 event/uin 列，改为单表过滤查询（提取 detail 中的 player_start_time）
 *   以测试 variant_get 在谓词过滤场景的性能
 */
public class FullPerfTest2 {

    static final String[] JSON_TABLES = {
        "srccat.gamedb.game_action_logs",
        "srccat.gamedb.game_action_logs_bucket"
    };

    static final String[] VARIANT_TABLES = {
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
                .appName("Full-PerfTest2-Join-Query")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== 性能对比测试 Round 2: Self-Join 查询 ===");
        System.out.println("Spark 版本: " + spark.version());

        // JSON 表：完整 self-join 查询
        for (String table : JSON_TABLES) {
            System.out.println("\n--- " + table + " ---");
            for (String aid : AIDS) {
                String sql =
                    "SELECT " +
                    "  date_trunc('day', a.tm) AS time, " +
                    "  a.event, " +
                    "  b.event, " +
                    "  unix_timestamp(b.tm) - " +
                    "  CAST(get_json_object(a.detail, '$.player_start_time') AS BIGINT) " +
                    "FROM " +
                    "  " + table + " a, " + table + " b " +
                    "WHERE " +
                    "  a.uin = b.uin " +
                    "  AND a.aid = b.aid " +
                    "  AND a.aid = " + aid + " " +
                    "  AND a.event = 'player_enter' AND a.uin != 0 AND a.aid != 0 " +
                    "  AND b.event = 'player_exit' " +
                    "  AND CAST(get_json_object(a.detail, '$.player_start_time') AS BIGINT) != 0 " +
                    "  AND CAST( " +
                    "    unix_timestamp(b.tm) - " +
                    "    CAST(get_json_object(a.detail, '$.player_start_time') AS BIGINT) " +
                    "  AS BIGINT) BETWEEN 1 AND 10799 " +
                    "ORDER BY time " +
                    "LIMIT 100";

                runQuery(spark, table, aid, sql);
            }
        }

        // Variant 表：等价查询（从 detail 提取 player_start_time 做过滤和计算）
        // 注意：Variant 表没有 uin/event 独立列，无法做完全等价的 self-join
        // 改为单表查询：过滤 player_start_time 并做时间计算
        for (String table : VARIANT_TABLES) {
            System.out.println("\n--- " + table + " ---");
            for (String aid : AIDS) {
                String sql =
                    "SELECT " +
                    "  date_trunc('day', tm) AS time, " +
                    "  variant_get(detail, '$.player_start_time', 'LONG') AS player_start_time, " +
                    "  unix_timestamp(tm) - CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT) AS duration " +
                    "FROM " + table + " " +
                    "WHERE " +
                    "  aid = " + aid + " " +
                    "  AND CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT) != 0 " +
                    "  AND CAST( " +
                    "    unix_timestamp(tm) - " +
                    "    CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT) " +
                    "  AS BIGINT) BETWEEN 1 AND 10799 " +
                    "ORDER BY time " +
                    "LIMIT 100";

                runQuery(spark, table, aid, sql);
            }
        }

        System.out.println("\n=== 性能对比测试 Round 2 完成 ===");
        spark.stop();
    }

    static void runQuery(SparkSession spark, String table, String aid, String sql) {
        long start = System.currentTimeMillis();
        try {
            spark.sql(sql).collect();
        } catch (Exception e) {
            System.out.println("QUERY_ERROR|" + table + "|aid=" + aid + "|error=" + e.getMessage());
            return;
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("QUERY_RESULT|" + table + "|aid=" + aid + "|ms=" + elapsed);
    }
}

package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Variant Shredding 性能测试
 *
 * 测试维度：
 *   Q1 — 单 aid 精确查询（验证 bucket pruning）
 *   Q2 — 全表扫描聚合（验证 shredding 列统计下推）
 *   Q3 — 多 aid 过滤（中间场景）
 *   Q4 — 嵌套字段聚合（验证 shredding 对嵌套字段的效果）
 *
 * 每个查询在两张表上各跑 3 次取均值。
 */
public class PerfTest {

    private static final Logger log = LoggerFactory.getLogger(PerfTest.class);

    private static final String CATALOG = "s3tablesbucket";
    private static final String NAMESPACE = "variant_shredding_test";
    private static final String TABLE_NO_BUCKET = NAMESPACE + ".variant_no_bucket";
    private static final String TABLE_WITH_BUCKET = NAMESPACE + ".variant_with_bucket";
    private static final int RUNS = 3;

    private static final String[] AIDS = {
            "aid_001", "aid_002", "aid_003", "aid_004", "aid_005",
            "aid_006", "aid_007", "aid_008", "aid_009", "aid_010"
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Variant-Shredding-PerfTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark 版本: {}", spark.version());

        Map<String, Map<String, Long>> results = new LinkedHashMap<>();

        log.info("======= 开始 VARIANT Shredding 性能测试 =======");

        // Q1: 单 aid 精确查询
        log.info("--- Q1: 单 aid 精确查询 ---");
        long q1NoTotal = 0, q1WithTotal = 0;
        for (String aid : AIDS) {
            String q = "SELECT aid, COUNT(*) AS cnt, " +
                    "SUM(variant_get(payload, '$.price', 'DOUBLE')) AS total_price, " +
                    "AVG(variant_get(payload, '$.score', 'DOUBLE')) AS avg_score " +
                    "FROM %s WHERE aid = '" + aid + "' GROUP BY aid";
            long avgNo = bench(spark, String.format(q, TABLE_NO_BUCKET), "Q1_no_" + aid);
            long avgWith = bench(spark, String.format(q, TABLE_WITH_BUCKET), "Q1_with_" + aid);
            q1NoTotal += avgNo;
            q1WithTotal += avgWith;
            results.put("Q1_" + aid, Map.of("no_bucket", avgNo, "with_bucket", avgWith));
        }

        // Q2: 全表扫描聚合
        log.info("--- Q2: 全表扫描聚合 ---");
        String q2 = "SELECT aid, COUNT(*) AS cnt, " +
                "SUM(variant_get(payload, '$.price', 'DOUBLE')) AS total_price, " +
                "AVG(variant_get(payload, '$.score', 'DOUBLE')) AS avg_score " +
                "FROM %s GROUP BY aid ORDER BY total_price DESC";
        long q2No = bench(spark, String.format(q2, TABLE_NO_BUCKET), "Q2_no");
        long q2With = bench(spark, String.format(q2, TABLE_WITH_BUCKET), "Q2_with");
        results.put("Q2_full_scan", Map.of("no_bucket", q2No, "with_bucket", q2With));

        // Q3: 多 aid 过滤
        log.info("--- Q3: 多 aid 过滤（5个）---");
        String q3 = "SELECT aid, COUNT(*) AS cnt, " +
                "SUM(variant_get(payload, '$.quantity', 'INT')) AS total_qty, " +
                "AVG(variant_get(payload, '$.price', 'DOUBLE')) AS avg_price " +
                "FROM %s WHERE aid IN ('aid_001','aid_002','aid_003','aid_004','aid_005') GROUP BY aid";
        long q3No = bench(spark, String.format(q3, TABLE_NO_BUCKET), "Q3_no");
        long q3With = bench(spark, String.format(q3, TABLE_WITH_BUCKET), "Q3_with");
        results.put("Q3_multi_aid", Map.of("no_bucket", q3No, "with_bucket", q3With));

        // Q4: 嵌套字段聚合
        log.info("--- Q4: 嵌套字段聚合（geo.lat）---");
        String q4 = "SELECT aid, COUNT(*) AS cnt, " +
                "AVG(variant_get(payload, '$.geo.lat', 'DOUBLE')) AS avg_lat, " +
                "AVG(variant_get(payload, '$.geo.lon', 'DOUBLE')) AS avg_lon " +
                "FROM %s WHERE aid = 'aid_001' GROUP BY aid";
        long q4No = bench(spark, String.format(q4, TABLE_NO_BUCKET), "Q4_no");
        long q4With = bench(spark, String.format(q4, TABLE_WITH_BUCKET), "Q4_with");
        results.put("Q4_nested", Map.of("no_bucket", q4No, "with_bucket", q4With));

        // 汇总输出
        log.info("======= 测试结果汇总 =======");
        log.info("{:<20} {:>12} {:>12} {:>8}", "查询", "无分桶(ms)", "分桶(ms)", "加速比");
        log.info("-".repeat(60));

        long q1NoAvg = q1NoTotal / AIDS.length;
        long q1WithAvg = q1WithTotal / AIDS.length;
        printRow("Q1 单aid(均值)", q1NoAvg, q1WithAvg);
        printRow("Q2 全表扫描", q2No, q2With);
        printRow("Q3 多aid过滤", q3No, q3With);
        printRow("Q4 嵌套字段", q4No, q4With);

        spark.stop();
        log.info("✅ 测试完成");
    }

    static long bench(SparkSession spark, String sql, String label) {
        long total = 0;
        for (int i = 1; i <= RUNS; i++) {
            long start = System.currentTimeMillis();
            spark.sql(sql).collect();
            long elapsed = System.currentTimeMillis() - start;
            log.info("  [{} run{}] {} ms", label, i, elapsed);
            total += elapsed;
        }
        long avg = total / RUNS;
        log.info("  [{}] 均值: {} ms", label, avg);
        return avg;
    }

    static void printRow(String name, long noMs, long withMs) {
        String ratio = withMs > 0 ? String.format("%.2fx", (double) noMs / withMs) : "N/A";
        log.info("{:<20} {:>12} {:>12} {:>8}", name, noMs, withMs, ratio);
    }
}

package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ETL: game_action_logs → game_action_logs_bucket_variant
 * 字段完全一致，detail 从 STRING → parse_json → VARIANT
 */
public class VariantMirrorEtl {

    private static final Logger log = LoggerFactory.getLogger(VariantMirrorEtl.class);

    private static final String SRC_TABLE = "srccat.gamedb.game_action_logs";
    private static final String DST_TABLE = "srccat.gamedb.game_action_logs_bucket_variant";

    // 源表日期范围
    private static final LocalDate START_DATE = LocalDate.of(2026, 4, 28);
    private static final LocalDate END_DATE = LocalDate.of(2026, 5, 29);

    private static final int REPARTITION_COUNT = 100;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ETL-Variant-Mirror")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark: {}", spark.version());

        List<LocalDate> allDates = getAllDates();
        log.info("总天数: {} ({} ~ {})", allDates.size(), allDates.get(0), allDates.get(allDates.size() - 1));

        // 断点续跑
        Set<String> done = getWrittenDates(spark, DST_TABLE);
        List<LocalDate> pending = allDates.stream()
                .filter(d -> !done.contains(d.toString()))
                .collect(Collectors.toList());
        log.info("已完成: {}, 待处理: {}", done.size(), pending.size());

        for (int i = 0; i < pending.size(); i++) {
            LocalDate dt = pending.get(i);
            log.info("进度 {}/{}: {}", i + 1, pending.size(), dt);
            processDay(spark, dt.toString());
        }

        long total = spark.sql("SELECT COUNT(*) FROM " + DST_TABLE).collectAsList().get(0).getLong(0);
        log.info("完成！目标表总行数: {}", String.format("%,d", total));
        spark.stop();
    }

    static void processDay(SparkSession spark, String date) {
        String sql = String.format(
            "SELECT aid, event, uin, roomid, lv, mode, version, area, tm, log_id, " +
            "parse_json(detail) AS detail " +
            "FROM %s WHERE CAST(tm AS DATE) = '%s'",
            SRC_TABLE, date);

        Dataset<Row> df = spark.sql(sql).repartition(REPARTITION_COUNT);
        try {
            df.writeTo(DST_TABLE).append();
        } catch (Exception e) {
            throw new RuntimeException("写入失败: " + date, e);
        }
        log.info("{} 写入完成", date);
    }

    static Set<String> getWrittenDates(SparkSession spark, String table) {
        try {
            // 用 snapshot 数量判断是否已有数据，避免全表扫描
            long count = spark.sql("SELECT COUNT(*) FROM " + table).collectAsList().get(0).getLong(0);
            if (count == 0) return Collections.emptySet();
            // 用 partition metadata 获取已写入的日期（更轻量）
            List<Row> rows = spark.sql(
                "SELECT DISTINCT CAST(tm AS DATE) AS dt FROM " + table +
                " WHERE tm >= '2026-04-28' AND tm < '2026-05-30'"
            ).collectAsList();
            return rows.stream().map(r -> r.get(0).toString()).collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("获取已写入日期失败: {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    static List<LocalDate> getAllDates() {
        List<LocalDate> dates = new ArrayList<>();
        LocalDate d = START_DATE;
        while (!d.isAfter(END_DATE)) {
            dates.add(d);
            d = d.plusDays(1);
        }
        return dates;
    }
}

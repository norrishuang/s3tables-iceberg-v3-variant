package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ETL：game_action_logs → 2 张 Variant Shredding 对比表
 *   1. variant_test_no_partition     — VARIANT + hours(tm)，无 bucket
 *   2. variant_test_with_bucket      — VARIANT + hours(tm) + bucket(64,aid)
 *
 * 源表: srccat.gamedb.game_action_logs（iceberg-data-812046859005，~40亿行）
 * 目标: dstcat.iceberg_v3_test.variant_test_no_partition
 *       dstcat.iceberg_v3_test.variant_test_with_bucket
 *
 * 字段映射: log_id→event_id, aid→aid, tm→tm, parse_json(detail)→detail
 * 按天分批处理，cache 后写两张表，支持断点续跑。
 *
 * Catalog 配置通过 sparkConf（SparkApplication YAML）传入：
 *   srccat = REST Catalog → iceberg-data-812046859005
 *   dstcat = REST Catalog → demo-tb-bucket-812046859005
 */
public class ShredingEtl {

    private static final Logger log = LoggerFactory.getLogger(ShredingEtl.class);

    // 双 Catalog：源和目标分别配置
    private static final String SRC_TABLE = "srccat.gamedb.game_action_logs";
    private static final String DST_NO_PART = "dstcat.iceberg_v3_test.variant_test_no_partition";
    private static final String DST_WITH_BKT = "dstcat.iceberg_v3_test.variant_test_with_bucket";

    // 硬编码日期范围（通过 Athena 确认：2026-04-28 ~ 2026-05-29，共 32 天）
    // 源表有 746 个 snapshot，Iceberg metadata scan 极慢，避免全表扫描获取日期
    private static final LocalDate START_DATE = LocalDate.of(2026, 4, 28);
    private static final LocalDate END_DATE = LocalDate.of(2026, 5, 29);

    // 每天数据写入时的 repartition 数量（避免分区过少导致文件过大）
    private static final int REPARTITION_COUNT = 200;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ETL-Variant-Shredding-FullLoad")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark 版本: {}", spark.version());

        // 确保目标表存在
        createTablesIfNotExist(spark);

        List<LocalDate> allDates = getAllDates();
        log.info("源表共 {} 天: {} ~ {}", allDates.size(), allDates.get(0), allDates.get(allDates.size() - 1));

        // 断点续跑：两张目标表都写完的日期才算"已完成"
        Set<String> done1 = getWrittenDates(spark, DST_NO_PART);
        Set<String> done2 = getWrittenDates(spark, DST_WITH_BKT);
        Set<String> done = new HashSet<>(done1);
        done.retainAll(done2);

        List<LocalDate> pending = allDates.stream()
                .filter(d -> !done.contains(d.toString()))
                .collect(Collectors.toList());
        log.info("总天数={}, 已完成={}, 待处理={}", allDates.size(), done.size(), pending.size());

        for (int i = 0; i < pending.size(); i++) {
            LocalDate dt = pending.get(i);
            log.info("进度: {}/{} — 开始处理 {} ...", i + 1, pending.size(), dt);
            processDay(spark, dt.toString());
            log.info("{} 处理完成", dt);
        }

        // 最终行数验证
        for (String tbl : List.of(DST_NO_PART, DST_WITH_BKT)) {
            long total = spark.read().table(tbl).count();
            log.info("最终行数 {}: {}", tbl, String.format("%,d", total));
        }

        spark.stop();
        log.info("全部处理完成");
    }

    /**
     * 确保目标 namespace 和表存在
     */
    static void createTablesIfNotExist(SparkSession spark) {
        log.info("确保目标 namespace 和表存在...");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS dstcat.iceberg_v3_test");

        spark.sql("CREATE TABLE IF NOT EXISTS " + DST_NO_PART + " (\n" +
                "    event_id BIGINT,\n" +
                "    aid      BIGINT,\n" +
                "    tm       TIMESTAMP,\n" +
                "    detail   VARIANT\n" +
                ") USING iceberg\n" +
                "PARTITIONED BY (hours(tm))\n" +
                "TBLPROPERTIES (\n" +
                "    'format-version' = '3',\n" +
                "    'write.parquet.compression-codec' = 'zstd'\n" +
                ")");
        log.info("表 {} 已就绪", DST_NO_PART);

        spark.sql("CREATE TABLE IF NOT EXISTS " + DST_WITH_BKT + " (\n" +
                "    event_id BIGINT,\n" +
                "    aid      BIGINT,\n" +
                "    tm       TIMESTAMP,\n" +
                "    detail   VARIANT\n" +
                ") USING iceberg\n" +
                "PARTITIONED BY (hours(tm), bucket(64, aid))\n" +
                "TBLPROPERTIES (\n" +
                "    'format-version' = '3',\n" +
                "    'write.parquet.compression-codec' = 'zstd'\n" +
                ")");
        log.info("表 {} 已就绪", DST_WITH_BKT);
    }

    /**
     * 硬编码日期列表（避免对 40 亿行源表做 DISTINCT 全表扫描）
     */
    static List<LocalDate> getAllDates() {
        List<LocalDate> dates = new ArrayList<>();
        LocalDate d = START_DATE;
        while (!d.isAfter(END_DATE)) {
            dates.add(d);
            d = d.plusDays(1);
        }
        return dates;
    }

    /**
     * 获取目标表已写入的日期集合，表为空或不存在时返回空集
     */
    static Set<String> getWrittenDates(SparkSession spark, String table) {
        try {
            return spark.sql(
                    "SELECT DISTINCT DATE(tm) AS dt FROM " + table + " ORDER BY dt"
            ).collectAsList().stream()
                    .map(r -> r.getDate(0).toString())
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("读取目标表日期失败，视为空表: {} ({})", table, e.getMessage());
            return Collections.emptySet();
        }
    }

    /**
     * 处理单天数据：从源表读 → cache → 写入两张目标表
     */
    static void processDay(SparkSession spark, String dt) {
        // parse_json(detail) 将 STRING 转成 VARIANT 类型
        Dataset<Row> df = spark.sql(
                "SELECT " +
                "    log_id AS event_id, aid, tm, parse_json(detail) AS detail " +
                "FROM " + SRC_TABLE + " " +
                "WHERE DATE(tm) = DATE('" + dt + "')"
        );

        // 使用 MEMORY_AND_DISK 避免内存不足时 OOM
        df.persist(StorageLevel.MEMORY_AND_DISK());
        long cnt = df.count();
        log.info("  {} 共 {} 行", dt, String.format("%,d", cnt));

        try {
            // 写入表1：hours(tm) 分区，指定 repartition 数量避免文件过大
            df.repartition(REPARTITION_COUNT, functions.hour(functions.col("tm")))
                    .writeTo(DST_NO_PART)
                    .append();
            log.info("  {} → {} 写入完成", dt, DST_NO_PART);

            // 写入表2：hours(tm) + bucket(64, aid) 分区
            df.repartition(REPARTITION_COUNT, functions.hour(functions.col("tm")), functions.col("aid").mod(64))
                    .writeTo(DST_WITH_BKT)
                    .append();
            log.info("  {} → {} 写入完成", dt, DST_WITH_BKT);
        } catch (Exception e) {
            throw new RuntimeException("写入失败 " + dt + ": " + e.getMessage(), e);
        }

        df.unpersist();
    }
}

package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ETL: 源表 detail STRING → parse_json → VARIANT 写入目标表
 *
 * 参数格式 (通过 SparkApplication arguments 传入):
 *   --src <catalog.namespace.table>    源表 (默认: srccat.gamedb.game_action_logs)
 *   --dst <catalog.namespace.table>    目标表 (必填)
 *   --start <yyyy-MM-dd>              起始日期 (默认: 2026-04-28)
 *   --end <yyyy-MM-dd>                结束日期 (默认: 2026-05-29)
 *   --repartition <N>                 写入分区数 (默认: 100)
 *   --mode <variant|json>             写入模式: variant=parse_json转换, json=原样复制 (默认: variant)
 *
 * 示例:
 *   --dst srccat.gamedb.game_action_logs_8b_variant --mode variant
 *   --dst srccat.gamedb.game_action_logs_8b_json --mode json
 */
public class VariantMirrorEtl {

    private static final Logger log = LoggerFactory.getLogger(VariantMirrorEtl.class);

    public static void main(String[] args) {
        Map<String, String> params = parseArgs(args);

        String srcTable = params.getOrDefault("src", "srccat.gamedb.game_action_logs");
        String dstTable = params.get("dst");
        String mode = params.getOrDefault("mode", "variant");
        LocalDate startDate = LocalDate.parse(params.getOrDefault("start", "2026-04-28"));
        LocalDate endDate = LocalDate.parse(params.getOrDefault("end", "2026-05-29"));
        int repartition = Integer.parseInt(params.getOrDefault("repartition", "100"));

        if (dstTable == null) {
            System.err.println("错误: 必须指定目标表 --dst <catalog.namespace.table>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("ETL-Variant-Mirror")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        log.info("Spark: {}", spark.version());
        log.info("SRC: {}, DST: {}, MODE: {}", srcTable, dstTable, mode);
        log.info("日期范围: {} ~ {}, repartition: {}", startDate, endDate, repartition);

        List<LocalDate> allDates = getAllDates(startDate, endDate);
        log.info("总天数: {}", allDates.size());

        // 断点续跑
        Set<String> done = getWrittenDates(spark, dstTable, startDate, endDate);
        List<LocalDate> pending = allDates.stream()
                .filter(d -> !done.contains(d.toString()))
                .collect(Collectors.toList());
        log.info("已完成: {}, 待处理: {}", done.size(), pending.size());

        for (int i = 0; i < pending.size(); i++) {
            LocalDate dt = pending.get(i);
            log.info("进度 {}/{}: {}", i + 1, pending.size(), dt);
            processDay(spark, srcTable, dstTable, dt.toString(), repartition, mode);
        }

        long total = spark.sql("SELECT COUNT(*) FROM " + dstTable).collectAsList().get(0).getLong(0);
        log.info("完成！目标表总行数: {}", String.format("%,d", total));
        spark.stop();
    }

    static void processDay(SparkSession spark, String srcTable, String dstTable, String date, int repartition, String mode) {
        String detailExpr = "variant".equals(mode) ? "parse_json(detail) AS detail" : "detail";
        String sql = String.format(
            "SELECT aid, event, uin, roomid, lv, mode, version, area, tm, log_id, %s " +
            "FROM %s WHERE CAST(tm AS DATE) = '%s'",
            detailExpr, srcTable, date);

        Dataset<Row> df = spark.sql(sql).repartition(repartition);
        try {
            df.writeTo(dstTable).append();
        } catch (Exception e) {
            throw new RuntimeException("写入失败: " + date, e);
        }
        log.info("{} 写入完成", date);
    }

    static Set<String> getWrittenDates(SparkSession spark, String table, LocalDate start, LocalDate end) {
        try {
            long count = spark.sql("SELECT COUNT(*) FROM " + table).collectAsList().get(0).getLong(0);
            if (count == 0) return Collections.emptySet();
            List<Row> rows = spark.sql(String.format(
                "SELECT DISTINCT CAST(tm AS DATE) AS dt FROM %s WHERE tm >= '%s' AND tm < '%s'",
                table, start, end.plusDays(1)
            )).collectAsList();
            return rows.stream().map(r -> r.get(0).toString()).collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("获取已写入日期失败: {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    static List<LocalDate> getAllDates(LocalDate start, LocalDate end) {
        List<LocalDate> dates = new ArrayList<>();
        LocalDate d = start;
        while (!d.isAfter(end)) {
            dates.add(d);
            d = d.plusDays(1);
        }
        return dates;
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--src":
                    params.put("src", args[++i]);
                    break;
                case "--dst":
                    params.put("dst", args[++i]);
                    break;
                case "--start":
                    params.put("start", args[++i]);
                    break;
                case "--end":
                    params.put("end", args[++i]);
                    break;
                case "--repartition":
                    params.put("repartition", args[++i]);
                    break;
                case "--mode":
                    params.put("mode", args[++i]);
                    break;
                default:
                    log.warn("未知参数: {}", args[i]);
            }
        }
        return params;
    }
}

package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ETL：game_action_logs → 2 张 Shredding 对比表
 *   1. game_action_logs_variant_shredded     — VARIANT + hours(tm)，无 bucket
 *   2. game_action_logs_bkt_variant_shredded — VARIANT + hours(tm) + bucket(64,aid)
 *
 * 所有 Catalog/Iceberg 配置通过外部 sparkConf（SparkApplication YAML 的 sparkConf 段）传入，
 * 代码内不硬编码任何 catalog 参数，保持与 PySpark 版完全等价的业务逻辑。
 * 支持断点续跑：已写入两张目标表的日期会被跳过。
 */
public class ShredingEtl {

    private static final Logger log = LoggerFactory.getLogger(ShredingEtl.class);

    static final String CATALOG   = "s3tablesbucket";
    static final String NAMESPACE = "gamedb";
    static final String SRC_TABLE = "game_action_logs";
    static final String DST1      = "game_action_logs_variant";
    static final String DST2      = "game_action_logs_bucket_variant";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("ETL-Shredding-From-Source")
                .getOrCreate();

        // 降低 Spark 内部日志噪音
        spark.sparkContext().setLogLevel("WARN");
        log.info("Spark 版本: {}", spark.version());

        // 获取源表所有日期（升序）
        List<String> allDates = getAllDates(spark);
        log.info("源表共 {} 天: {} ~ {}",
                allDates.size(), allDates.get(0), allDates.get(allDates.size() - 1));

        // 断点续跑：两张目标表都写完的日期才算"已完成"
        Set<String> done1 = getWrittenDates(spark, DST1);
        Set<String> done2 = getWrittenDates(spark, DST2);
        Set<String> done  = new HashSet<>(done1);
        done.retainAll(done2);

        List<String> pending = allDates.stream()
                .filter(d -> !done.contains(d))
                .collect(Collectors.toList());
        log.info("总天数={}, 已完成={}, 待处理={}", allDates.size(), done.size(), pending.size());

        for (String dt : pending) {
            log.info("开始处理 {} ...", dt);
            processDay(spark, dt);
            log.info("{} 处理完成", dt);
        }

        spark.stop();
        log.info("全部处理完成");
    }


    /**
     * 获取源表所有日期（升序，返回 yyyy-MM-dd 字符串列表）
     */
    static List<String> getAllDates(SparkSession spark) {
        String src = fqn(SRC_TABLE);
        return spark.sql(
                "SELECT DISTINCT DATE(tm) AS dt FROM " + src + " ORDER BY dt"
        ).collectAsList().stream()
                .map(r -> r.getDate(0).toString())   // java.sql.Date.toString() = "yyyy-MM-dd"
                .collect(Collectors.toList());
    }

    /**
     * 获取目标表已写入的日期集合，表为空或不存在时返回空集
     */
    static Set<String> getWrittenDates(SparkSession spark, String table) {
        try {
            return spark.sql(
                    "SELECT DISTINCT DATE(tm) AS dt FROM " + fqn(table) + " ORDER BY dt"
            ).collectAsList().stream()
                    .map(r -> r.getDate(0).toString())
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("读取目标表日期失败，视为空表（原因：{}）", e.getMessage());
            return Collections.emptySet();
        }
    }

    /**
     * 处理单天数据：从源表读 → 写入两张目标表
     */
    static void processDay(SparkSession spark, String dt) {
        String src = fqn(SRC_TABLE);
        String t1  = fqn(DST1);
        String t2  = fqn(DST2);

        // parse_json(detail) 将 STRING 转成 VARIANT 类型
        Dataset<Row> df = spark.sql(
            "SELECT\n" +
            "    aid, event, uin, roomid, lv, mode, version, area, tm, log_id,\n" +
            "    parse_json(detail) AS detail\n" +
            "FROM " + src + "\n" +
            "WHERE DATE(tm) = DATE('" + dt + "')"
        );

        // 缓存，避免写两张表时触发两次全量 S3 扫描
        df.cache();
        long cnt = df.count();
        log.info("  {} 共 {} 行", dt, cnt);

        try {
            df.writeTo(t1).append();
            df.writeTo(t2).append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new RuntimeException("目标表不存在，请先执行 createTables: " + e.getMessage(), e);
        }

        df.unpersist();
    }

    /**
     * 拼接三段限定名 catalog.namespace.table
     */
    static String fqn(String table) {
        return CATALOG + "." + NAMESPACE + "." + table;
    }
}

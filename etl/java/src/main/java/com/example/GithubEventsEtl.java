package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL: S3 JSON gz → S3 Tables (github.events_json 或 github.events_variant)
 *
 * 参数:
 *   --src <s3a://bucket/path/>       JSON gz 源路径 (必填)
 *   --dst <catalog.ns.table>         目标表 (必填)
 *   --mode <json|variant>            写入模式 (必填)
 *   --repartition <N>               写入分区数 (默认: 200)
 *
 * mode=json:    actor/repo/payload/org → to_json() 转为 STRING
 * mode=variant: actor/repo/payload/org → to_json() → parse_json() 转为 VARIANT
 */
public class GithubEventsEtl {

    private static final Logger log = LoggerFactory.getLogger(GithubEventsEtl.class);

    public static void main(String[] args) {
        Map<String, String> params = parseArgs(args);

        String srcPath = params.get("src");
        String dstTable = params.get("dst");
        String mode = params.getOrDefault("mode", "variant");
        int repartition = Integer.parseInt(params.getOrDefault("repartition", "200"));

        if (srcPath == null || dstTable == null) {
            System.err.println("用法: --src <s3a://path/> --dst <catalog.ns.table> --mode <json|variant>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("ETL-GitHub-Events-" + mode)
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        log.info("Spark: {}", spark.version());
        log.info("SRC: {}, DST: {}, MODE: {}, repartition: {}", srcPath, dstTable, mode, repartition);

        // 读取 JSON gz (每行一个 JSON 对象)
        Dataset<Row> raw = spark.read().json(srcPath);
        long totalCount = raw.count();
        log.info("源数据总行数: {}", String.format("%,d", totalCount));

        // 根据 mode 构建不同的 SELECT 表达式
        String actorExpr, repoExpr, payloadExpr, orgExpr;
        if ("variant".equals(mode)) {
            actorExpr = "to_json(actor) AS actor";
            repoExpr = "to_json(repo) AS repo";
            payloadExpr = "parse_json(to_json(payload)) AS payload";
            orgExpr = "to_json(org) AS org";
        } else {
            actorExpr = "to_json(actor) AS actor";
            repoExpr = "to_json(repo) AS repo";
            payloadExpr = "to_json(payload) AS payload";
            orgExpr = "to_json(org) AS org";
        }

        Dataset<Row> df = raw.selectExpr(
            "id",
            "type",
            "`public`",
            "to_timestamp(created_at) AS created_at",
            actorExpr,
            repoExpr,
            payloadExpr,
            orgExpr
        ).repartition(repartition);

        log.info("开始写入 {} ...", dstTable);
        try {
            df.writeTo(dstTable).append();
        } catch (Exception e) {
            throw new RuntimeException("写入失败", e);
        }

        long written = spark.sql("SELECT COUNT(*) FROM " + dstTable).collectAsList().get(0).getLong(0);
        log.info("完成！目标表总行数: {}", String.format("%,d", written));
        spark.stop();
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--src": params.put("src", args[++i]); break;
                case "--dst": params.put("dst", args[++i]); break;
                case "--mode": params.put("mode", args[++i]); break;
                case "--repartition": params.put("repartition", args[++i]); break;
                default: log.warn("未知参数: {}", args[i]);
            }
        }
        return params;
    }
}

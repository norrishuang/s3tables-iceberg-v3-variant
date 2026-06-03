package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * 诊断：确认 variant_get 是否利用了 shredded 列裁剪
 */
public class VariantDiag {

    static final String JSON_TABLE = "srccat.gamedb.game_action_logs_8b_json";
    static final String VARIANT_TABLE = "srccat.gamedb.game_action_logs_8b_variant";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Variant-Diag")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Variant Shredding 诊断 ===");
        System.out.println("Spark: " + spark.version());

        // 1. 查看 variant 表 schema (确认 shredding 列是否存在)
        System.out.println("\n=== 1. Variant 表 Schema ===");
        try {
            Dataset<Row> schema = spark.sql("DESCRIBE EXTENDED " + VARIANT_TABLE);
            schema.show(100, false);
        } catch (Exception e) {
            System.out.println("DESCRIBE ERROR: " + e.getMessage());
        }

        // 2. 查看 Parquet 文件的列信息 (通过 files metadata)
        System.out.println("\n=== 2. Variant 表 Parquet 文件信息 ===");
        try {
            Dataset<Row> files = spark.sql(
                "SELECT file_path, file_size_in_bytes, record_count " +
                "FROM " + VARIANT_TABLE + ".files LIMIT 3");
            files.show(false);

            // 取第一个文件路径，尝试读取 parquet schema
            String firstFile = files.collectAsList().get(0).getString(0);
            System.out.println("First file: " + firstFile);

            Dataset<Row> pqSchema = spark.read().parquet(firstFile);
            System.out.println("\nParquet physical schema:");
            pqSchema.printSchema();
        } catch (Exception e) {
            System.out.println("FILE SCHEMA ERROR: " + e.getMessage());
            e.printStackTrace(System.out);
        }

        // 3. EXPLAIN variant_get 查询
        System.out.println("\n=== 3. EXPLAIN: variant_get 查询 ===");
        try {
            String sql = "SELECT variant_get(detail, '$.map_id', 'STRING') AS map_id " +
                         "FROM " + VARIANT_TABLE + " WHERE aid = 5994833300571649523 LIMIT 10";
            Dataset<Row> plan = spark.sql("EXPLAIN FORMATTED " + sql);
            plan.show(1, false);
        } catch (Exception e) {
            System.out.println("EXPLAIN ERROR: " + e.getMessage());
        }

        // 4. EXPLAIN JSON 查询做对比
        System.out.println("\n=== 4. EXPLAIN: get_json_object 查询 ===");
        try {
            String sql = "SELECT get_json_object(detail, '$.map_id') AS map_id " +
                         "FROM " + JSON_TABLE + " WHERE aid = 5994833300571649523 LIMIT 10";
            Dataset<Row> plan = spark.sql("EXPLAIN FORMATTED " + sql);
            plan.show(1, false);
        } catch (Exception e) {
            System.out.println("EXPLAIN ERROR: " + e.getMessage());
        }

        // 5. 尝试 dot notation 访问
        System.out.println("\n=== 5. 尝试 dot notation: detail.map_id ===");
        try {
            String sql = "SELECT detail.map_id FROM " + VARIANT_TABLE +
                         " WHERE aid = 5994833300571649523 LIMIT 10";
            Dataset<Row> result = spark.sql(sql);
            result.show(false);
            System.out.println("DOT NOTATION: 成功！");

            // EXPLAIN dot notation
            Dataset<Row> plan = spark.sql("EXPLAIN FORMATTED " + sql);
            plan.show(1, false);
        } catch (Exception e) {
            System.out.println("DOT NOTATION ERROR: " + e.getMessage());
        }

        // 6. 对比 dot notation vs variant_get 性能 (单次)
        System.out.println("\n=== 6. 性能对比: dot notation vs variant_get ===");
        String aid = "5994833300571649523";

        // variant_get
        long t1 = System.currentTimeMillis();
        try {
            spark.sql("SELECT variant_get(detail, '$.map_id', 'STRING') FROM " +
                      VARIANT_TABLE + " WHERE aid = " + aid).count();
        } catch (Exception e) { System.out.println("variant_get count error: " + e.getMessage()); }
        long varGetMs = System.currentTimeMillis() - t1;
        System.out.println("variant_get time: " + varGetMs + "ms");

        // dot notation
        long t2 = System.currentTimeMillis();
        try {
            spark.sql("SELECT detail.map_id FROM " +
                      VARIANT_TABLE + " WHERE aid = " + aid).count();
        } catch (Exception e) { System.out.println("dot notation count error: " + e.getMessage()); }
        long dotMs = System.currentTimeMillis() - t2;
        System.out.println("dot notation time: " + dotMs + "ms");

        // JSON baseline
        long t3 = System.currentTimeMillis();
        try {
            spark.sql("SELECT get_json_object(detail, '$.map_id') FROM " +
                      JSON_TABLE + " WHERE aid = " + aid).count();
        } catch (Exception e) { System.out.println("json count error: " + e.getMessage()); }
        long jsonMs = System.currentTimeMillis() - t3;
        System.out.println("get_json_object time: " + jsonMs + "ms");

        System.out.println("\nTIMING_SUMMARY|variant_get=" + varGetMs + "|dot_notation=" + dotMs + "|json=" + jsonMs);

        spark.stop();
    }
}

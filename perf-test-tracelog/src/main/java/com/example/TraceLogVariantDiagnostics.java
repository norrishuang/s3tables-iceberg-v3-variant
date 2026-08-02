package com.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class TraceLogVariantDiagnostics {
    private static final String VARIANT_TABLE = "s3tablesbucket.tracelog.otel_spans_v2";
    private static final String JSON_TABLE = "s3tablesbucket.tracelog.otel_spans_json_v2";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TraceLog-Variant-Diagnostics")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        inspectTable(spark, VARIANT_TABLE);
        inspectTable(spark, JSON_TABLE);
        testAttributePaths(spark);

        explain(spark, "VARIANT_Q3",
                "SELECT COUNT(*) AS cnt, " +
                "COUNT(variant_get(attributes, '$[''gen_ai.request.model'']', 'string')) AS model_cnt " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE variant_get(attributes, '$[''gen_ai.system'']', 'string') = 'anthropic'");
        explain(spark, "JSON_Q3",
                "SELECT COUNT(*) AS cnt, " +
                "COUNT(get_json_object(attributes, '$[''gen_ai.request.model'']')) AS model_cnt " +
                "FROM " + JSON_TABLE + " " +
                "WHERE get_json_object(attributes, '$[''gen_ai.system'']') = 'anthropic'");

        spark.stop();
    }

    private static void inspectTable(SparkSession spark, String table) {
        System.out.println("=== TABLE|" + table + " ===");
        spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().forEach(
                row -> System.out.println("PROPERTY|" + row.getString(0) + "|" + row.getString(1)));

        Row stats = spark.sql(
                "SELECT COUNT(*) AS file_count, SUM(file_size_in_bytes) AS total_bytes, " +
                "AVG(file_size_in_bytes) AS avg_bytes, SUM(record_count) AS records " +
                "FROM " + table + ".files").first();
        System.out.println("FILES|" + table + "|" + stats.getLong(0) + "|" +
                stats.getLong(1) + "|" + stats.getDouble(2) + "|" + stats.getLong(3));

        if (table.equals(VARIANT_TABLE)) {
            String path = spark.sql("SELECT file_path FROM " + table + ".files LIMIT 1")
                    .first().getString(0);
            String s3aPath = path.replaceFirst("^s3://", "s3a://");
            try (ParquetFileReader reader = ParquetFileReader.open(
                    HadoopInputFile.fromPath(new Path(s3aPath), spark.sparkContext().hadoopConfiguration()))) {
                System.out.println("PARQUET_SCHEMA|" + s3aPath + "|" +
                        reader.getFooter().getFileMetaData().getSchema());
            } catch (Exception e) {
                System.out.println("PARQUET_SCHEMA_ERROR|" + e.getMessage());
            }
        }
    }

    private static void explain(SparkSession spark, String label, String sql) {
        System.out.println("=== EXPLAIN|" + label + " ===");
        spark.sql("EXPLAIN FORMATTED " + sql).collectAsList().forEach(
                row -> System.out.println("PLAN|" + label + "|" + row.getString(0)));
    }

    private static void testAttributePaths(SparkSession spark) {
        System.out.println("=== ATTRIBUTE_PATHS ===");
        sample(spark, "VARIANT_RAW",
                "SELECT to_json(attributes) AS attributes_json FROM " + VARIANT_TABLE +
                " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "VARIANT_UNQUOTED",
                "SELECT try_variant_get(attributes, '$.gen_ai.request.model', 'string') AS value FROM " +
                VARIANT_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "VARIANT_DOUBLE_QUOTED",
                "SELECT try_variant_get(attributes, '$.\"gen_ai.request.model\"', 'string') AS value FROM " +
                VARIANT_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "VARIANT_BRACKET_QUOTED",
                "SELECT try_variant_get(attributes, '$[\"gen_ai.request.model\"]', 'string') AS value FROM " +
                VARIANT_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "VARIANT_BRACKET_SINGLE_QUOTED",
                "SELECT try_variant_get(attributes, '$[''gen_ai.request.model'']', 'string') AS value FROM " +
                VARIANT_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "JSON_UNQUOTED",
                "SELECT get_json_object(attributes, '$.gen_ai.request.model') AS value FROM " +
                JSON_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "JSON_BRACKET_QUOTED",
                "SELECT get_json_object(attributes, '$[\"gen_ai.request.model\"]') AS value FROM " +
                JSON_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
        sample(spark, "JSON_BRACKET_SINGLE_QUOTED",
                "SELECT get_json_object(attributes, '$[''gen_ai.request.model'']') AS value FROM " +
                JSON_TABLE + " WHERE attributes IS NOT NULL LIMIT 1");
    }

    private static void sample(SparkSession spark, String label, String sql) {
        try {
            Row row = spark.sql(sql).first();
            System.out.println("PATH|" + label + "|" + (row.isNullAt(0) ? "NULL" : row.get(0)));
        } catch (Exception e) {
            System.out.println("PATH_ERROR|" + label + "|" + e.getMessage());
        }
    }
}

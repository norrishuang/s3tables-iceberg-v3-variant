package com.example;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the JSON mirror table (otel_spans_json) from the existing Variant table.
 * Converts VARIANT columns to JSON STRING for performance comparison.
 */
public class SetupJsonTable {
    private static final Logger LOG = LoggerFactory.getLogger(SetupJsonTable.class);

    private static final String VARIANT_TABLE = "s3tablesbucket.tracelog.otel_spans";
    private static final String JSON_TABLE = "s3tablesbucket.tracelog.otel_spans_json";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TraceLog-PerfTest-Setup")
                .getOrCreate();

        LOG.info("=== Setting up JSON mirror table ===");

        // Drop if exists and recreate
        spark.sql("DROP TABLE IF EXISTS " + JSON_TABLE);
        LOG.info("Dropped existing table (if any): {}", JSON_TABLE);

        // Create JSON table from Variant table, converting VARIANT -> STRING
        String createSql = String.format(
            "CREATE TABLE %s " +
            "USING iceberg " +
            "PARTITIONED BY (days(start_time)) " +
            "TBLPROPERTIES ('format-version' = '3', 'write.parquet.compression-codec' = 'zstd') " +
            "AS SELECT " +
            "  trace_id, span_id, parent_span_id, name, kind, " +
            "  start_time, end_time, duration_ms, status_code, " +
            "  service_name, service_version, deployment_env, " +
            "  CAST(attributes AS STRING) AS attributes, " +
            "  CAST(events AS STRING) AS events, " +
            "  CAST(resource_attributes AS STRING) AS resource_attributes " +
            "FROM %s",
            JSON_TABLE, VARIANT_TABLE
        );

        LOG.info("Creating JSON table with SQL:\n{}", createSql);
        spark.sql(createSql);

        // Verify row counts
        long variantCount = spark.sql("SELECT COUNT(*) FROM " + VARIANT_TABLE).first().getLong(0);
        long jsonCount = spark.sql("SELECT COUNT(*) FROM " + JSON_TABLE).first().getLong(0);
        LOG.info("=== Setup complete ===");
        LOG.info("Variant table rows: {}", variantCount);
        LOG.info("JSON table rows:    {}", jsonCount);

        if (variantCount != jsonCount) {
            LOG.error("Row count mismatch! Variant={} vs JSON={}", variantCount, jsonCount);
        }

        spark.stop();
    }
}

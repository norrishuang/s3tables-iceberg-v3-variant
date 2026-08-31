package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageStats {
    private static final Logger LOG = LoggerFactory.getLogger(StorageStats.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TraceLog-StorageStats")
                .getOrCreate();

        String[] tables = {
            "s3tablesbucket.tracelog.otel_spans_v2",
            "s3tablesbucket.tracelog.otel_spans_json_v2"
        };

        for (String table : tables) {
            LOG.info("=== {} ===", table);

            // Row count
            long rows = spark.sql("SELECT COUNT(*) FROM " + table).first().getLong(0);
            LOG.info("  Rows: {}", rows);

            // Snapshot/files metadata
            try {
                Row stats = spark.sql(String.format(
                    "SELECT COUNT(*) AS file_count, " +
                    "  SUM(file_size_in_bytes) AS total_bytes, " +
                    "  AVG(file_size_in_bytes) AS avg_file_bytes, " +
                    "  SUM(record_count) AS total_records " +
                    "FROM %s.files", table
                )).first();
                LOG.info("  Files: {}", stats.getLong(0));
                LOG.info("  Total size (bytes): {}", stats.get(1));
                LOG.info("  Total size (GB): {}", String.format("%.2f", stats.getLong(1) / 1073741824.0));
                LOG.info("  Avg file size (MB): {}", String.format("%.2f", stats.getDouble(2) / 1048576.0));
                LOG.info("  Total records: {}", stats.get(3));
            } catch (Exception e) {
                LOG.info("  Files metadata not available via .files: {}", e.getMessage());
                // Try snapshots
                try {
                    spark.sql("SELECT * FROM " + table + ".snapshots").show(false);
                } catch (Exception e2) {
                    LOG.info("  Snapshots also not available: {}", e2.getMessage());
                }
            }
        }

        spark.stop();
    }
}

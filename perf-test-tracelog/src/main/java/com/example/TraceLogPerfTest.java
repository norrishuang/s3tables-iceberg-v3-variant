package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Performance test comparing Variant+Shredding vs JSON STRING
 * for agent trace log (OTel spans) queries.
 */
public class TraceLogPerfTest {
    private static final Logger LOG = LoggerFactory.getLogger(TraceLogPerfTest.class);

    private static final String VARIANT_TABLE = "s3tablesbucket.tracelog.otel_spans_v2";
    private static final String JSON_TABLE = "s3tablesbucket.tracelog.otel_spans_json_v2";
    private static final int RUNS = 3;

    private static SparkSession spark;

    public static void main(String[] args) {
        spark = SparkSession.builder()
                .appName("TraceLog-PerfTest")
                .getOrCreate();

        LOG.info("=== Agent Trace Log Performance Test: Variant vs JSON ===");

        // Discover time range and a sample service/trace for parameterized queries
        Row meta = spark.sql(String.format(
            "SELECT MIN(start_time), MAX(start_time), " +
            "  (SELECT service_name FROM %s WHERE service_name IS NOT NULL LIMIT 1), " +
            "  (SELECT trace_id FROM %s WHERE trace_id IS NOT NULL LIMIT 1) " +
            "FROM %s", VARIANT_TABLE, VARIANT_TABLE, VARIANT_TABLE
        )).first();

        String startTs = meta.get(0).toString();
        String endTs = meta.get(1).toString();
        String sampleService = meta.getString(2);
        String sampleTraceId = meta.getString(3);

        long totalRows = spark.sql("SELECT COUNT(*) FROM " + VARIANT_TABLE).first().getLong(0);
        LOG.info("Data range: {} to {}", startTs, endTs);
        LOG.info("Total rows: {}", totalRows);
        LOG.info("Sample service: {}", sampleService);
        LOG.info("Sample trace_id: {}", sampleTraceId);

        List<String[]> results = new ArrayList<>();

        // Q1: Service filter + attribute extraction + aggregation
        // Aggregates token usage by service — tests shredding column read for numeric fields
        results.add(runQuery("Q1_attr_agg",
            "SELECT service_name, COUNT(*) AS cnt, " +
            "  SUM(variant_get(attributes, '$[''gen_ai.usage.input_tokens'']', 'int')) AS total_input, " +
            "  SUM(variant_get(attributes, '$[''gen_ai.usage.output_tokens'']', 'int')) AS total_output " +
            "FROM %s WHERE service_name = '%s' " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "GROUP BY service_name",
            "SELECT service_name, COUNT(*) AS cnt, " +
            "  SUM(CAST(get_json_object(attributes, '$[''gen_ai.usage.input_tokens'']') AS INT)) AS total_input, " +
            "  SUM(CAST(get_json_object(attributes, '$[''gen_ai.usage.output_tokens'']') AS INT)) AS total_output " +
            "FROM %s WHERE service_name = '%s' " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "GROUP BY service_name",
            sampleService, startTs, endTs
        ));

        // Q2: Full table aggregation — all services token stats
        results.add(runQuery("Q2_full_agg",
            "SELECT service_name, COUNT(*) AS cnt, " +
            "  SUM(variant_get(attributes, '$[''gen_ai.usage.input_tokens'']', 'int')) AS total_in, " +
            "  SUM(variant_get(attributes, '$[''gen_ai.usage.output_tokens'']', 'int')) AS total_out, " +
            "  AVG(duration_ms) AS avg_dur " +
            "FROM %s WHERE start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "GROUP BY service_name ORDER BY total_in DESC",
            "SELECT service_name, COUNT(*) AS cnt, " +
            "  SUM(CAST(get_json_object(attributes, '$[''gen_ai.usage.input_tokens'']') AS INT)) AS total_in, " +
            "  SUM(CAST(get_json_object(attributes, '$[''gen_ai.usage.output_tokens'']') AS INT)) AS total_out, " +
            "  AVG(duration_ms) AS avg_dur " +
            "FROM %s WHERE start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "GROUP BY service_name ORDER BY total_in DESC",
            startTs, endTs
        ));

        // Q3: Filter on variant attribute value — count matching rows
        results.add(runQuery("Q3_attr_filter",
            "SELECT COUNT(*) AS cnt, " +
            "  COUNT(variant_get(attributes, '$[''gen_ai.request.model'']', 'string')) AS model_cnt " +
            "FROM %s WHERE variant_get(attributes, '$[''gen_ai.system'']', 'string') = 'anthropic' " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s'",
            "SELECT COUNT(*) AS cnt, " +
            "  COUNT(get_json_object(attributes, '$[''gen_ai.request.model'']')) AS model_cnt " +
            "FROM %s WHERE get_json_object(attributes, '$[''gen_ai.system'']') = 'anthropic' " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s'",
            startTs, endTs
        ));

        // Q4: Slow spans — LIMIT 100 (safe for collect)
        results.add(runQuery("Q4_slow_spans",
            "SELECT name, service_name, " +
            "  variant_get(attributes, '$[''gen_ai.usage.input_tokens'']', 'int') AS input_tokens, " +
            "  variant_get(attributes, '$[''gen_ai.response.finish_reasons''][0]', 'string') AS finish_reason, " +
            "  duration_ms " +
            "FROM %s WHERE duration_ms > 5000 " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "ORDER BY duration_ms DESC LIMIT 100",
            "SELECT name, service_name, " +
            "  CAST(get_json_object(attributes, '$[''gen_ai.usage.input_tokens'']') AS INT) AS input_tokens, " +
            "  get_json_object(attributes, '$[''gen_ai.response.finish_reasons''][0]') AS finish_reason, " +
            "  duration_ms " +
            "FROM %s WHERE duration_ms > 5000 " +
            "  AND start_time >= timestamp '%s' AND start_time < timestamp '%s' " +
            "ORDER BY duration_ms DESC LIMIT 100",
            startTs, endTs
        ));

        // Q5: Trace reconstruction — point query (few rows per trace)
        results.add(runQuery("Q5_trace_lookup",
            "SELECT span_id, parent_span_id, name, duration_ms, " +
            "  variant_get(attributes, '$[''gen_ai.system'']', 'string') AS ai_system " +
            "FROM %s WHERE trace_id = '%s' ORDER BY start_time",
            "SELECT span_id, parent_span_id, name, duration_ms, " +
            "  get_json_object(attributes, '$[''gen_ai.system'']') AS ai_system " +
            "FROM %s WHERE trace_id = '%s' ORDER BY start_time",
            sampleTraceId
        ));

        // Print summary
        LOG.info("");
        LOG.info("╔═══════════════════════════════════════════════════════════════╗");
        LOG.info("║         PERFORMANCE TEST RESULTS: Variant vs JSON            ║");
        LOG.info("║         Data: {} rows                          ║", totalRows);
        LOG.info("╠════════════════╦══════════════╦══════════════╦═══════════════╣");
        LOG.info("║ Query          ║ Variant (ms) ║ JSON (ms)    ║ Speedup       ║");
        LOG.info("╠════════════════╬══════════════╬══════════════╬═══════════════╣");
        for (String[] r : results) {
            LOG.info("║ {}", r[0]);
        }
        LOG.info("╚════════════════╩══════════════╩══════════════╩═══════════════╝");

        spark.stop();
    }

    private static String[] runQuery(String label, String variantSqlTpl, String jsonSqlTpl, Object... params) {
        Object[] variantParams = new Object[params.length + 1];
        variantParams[0] = VARIANT_TABLE;
        System.arraycopy(params, 0, variantParams, 1, params.length);
        String variantSql = String.format(variantSqlTpl, variantParams);

        Object[] jsonParams = new Object[params.length + 1];
        jsonParams[0] = JSON_TABLE;
        System.arraycopy(params, 0, jsonParams, 1, params.length);
        String jsonSql = String.format(jsonSqlTpl, jsonParams);

        LOG.info("--- {} ---", label);
        LOG.info("  Variant SQL: {}", variantSql);
        LOG.info("  JSON SQL: {}", jsonSql);

        double variantMs = bench(variantSql, label + " [VARIANT]");
        double jsonMs = bench(jsonSql, label + " [JSON]");
        double speedup = jsonMs / variantMs;

        String line = String.format("%-14s ║ %10.0f   ║ %10.0f   ║ %8.2fx      ║",
                label, variantMs, jsonMs, speedup);
        LOG.info("  => Variant={}ms, JSON={}ms, Speedup={}x",
                String.format("%.0f", variantMs), String.format("%.0f", jsonMs), String.format("%.2f", speedup));
        return new String[]{line};
    }

    private static double bench(String sql, String label) {
        long total = 0;
        for (int i = 0; i < RUNS; i++) {
            long start = System.currentTimeMillis();
            spark.sql(sql).collect();
            long elapsed = System.currentTimeMillis() - start;
            total += elapsed;
            LOG.info("    {} run {}: {}ms", label, i + 1, elapsed);
        }
        double avg = (double) total / RUNS;
        LOG.info("    {} avg: {}ms", label, String.format("%.0f", avg));
        return avg;
    }
}

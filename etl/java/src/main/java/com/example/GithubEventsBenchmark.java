package com.example;

import org.apache.spark.sql.*;
import java.util.*;

/**
 * GitHub Events 查询性能对比测试
 * Variant 访问语法: variant_get(col, '$.path', 'type')
 */
public class GithubEventsBenchmark {

    private static final int WARMUP_RUNS = 1;
    private static final int BENCH_RUNS = 3;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("GitHub-Events-Benchmark")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // 预热 catalog
        spark.sql("SELECT 1 FROM s3t.github.events_json LIMIT 1").collect();
        spark.sql("SELECT 1 FROM s3t.github.events_variant LIMIT 1").collect();

        String[][] cases = {
            {"Case1_Predicate_Pushdown",
             "SELECT id, type, created_at FROM s3t.github.events_json WHERE type = 'IssueCommentEvent' AND get_json_object(payload, '$.action') = 'created'",
             "SELECT id, type, created_at FROM s3t.github.events_variant WHERE type = 'IssueCommentEvent' AND variant_get(payload, '$.action', 'string') = 'created'"},

            {"Case2_Deep_Nested_Filter",
             "SELECT id, created_at, get_json_object(payload, '$.issue.title') AS issue_title FROM s3t.github.events_json WHERE type = 'IssuesEvent' AND CAST(get_json_object(payload, '$.issue.comments') AS INT) > 10",
             "SELECT id, created_at, variant_get(payload, '$.issue.title', 'string') AS issue_title FROM s3t.github.events_variant WHERE type = 'IssuesEvent' AND variant_get(payload, '$.issue.comments', 'int') > 10"},

            {"Case3_Aggregation",
             "SELECT get_json_object(payload, '$.action') AS action, COUNT(*) AS cnt FROM s3t.github.events_json WHERE type = 'WatchEvent' OR type = 'ForkEvent' OR type = 'IssuesEvent' GROUP BY get_json_object(payload, '$.action') ORDER BY cnt DESC",
             "SELECT variant_get(payload, '$.action', 'string') AS action, COUNT(*) AS cnt FROM s3t.github.events_variant WHERE type = 'WatchEvent' OR type = 'ForkEvent' OR type = 'IssuesEvent' GROUP BY variant_get(payload, '$.action', 'string') ORDER BY cnt DESC"},

            {"Case4_Wide_Projection",
             "SELECT id, get_json_object(payload, '$.action') AS action, get_json_object(payload, '$.number') AS pr_number, get_json_object(payload, '$.pull_request.title') AS pr_title, get_json_object(payload, '$.pull_request.state') AS pr_state, get_json_object(payload, '$.pull_request.merged') AS pr_merged FROM s3t.github.events_json WHERE type = 'PullRequestEvent'",
             "SELECT id, variant_get(payload, '$.action', 'string') AS action, variant_get(payload, '$.number', 'int') AS pr_number, variant_get(payload, '$.pull_request.title', 'string') AS pr_title, variant_get(payload, '$.pull_request.state', 'string') AS pr_state, variant_get(payload, '$.pull_request.merged', 'boolean') AS pr_merged FROM s3t.github.events_variant WHERE type = 'PullRequestEvent'"},

            {"Case5_Numeric_Filter_Agg",
             "SELECT get_json_object(repo, '$.name') AS repo_name, COUNT(*) AS push_count, SUM(CAST(get_json_object(payload, '$.size') AS INT)) AS total_commits FROM s3t.github.events_json WHERE type = 'PushEvent' AND CAST(get_json_object(payload, '$.size') AS INT) > 10 GROUP BY get_json_object(repo, '$.name') ORDER BY total_commits DESC LIMIT 20",
             "SELECT get_json_object(repo, '$.name') AS repo_name, COUNT(*) AS push_count, SUM(variant_get(payload, '$.size', 'int')) AS total_commits FROM s3t.github.events_variant WHERE type = 'PushEvent' AND variant_get(payload, '$.size', 'int') > 10 GROUP BY get_json_object(repo, '$.name') ORDER BY total_commits DESC LIMIT 20"},

            {"Case6_Multi_Field_Extract",
             "SELECT id, get_json_object(payload, '$.action') AS action, get_json_object(payload, '$.ref_type') AS ref_type, get_json_object(payload, '$.ref') AS ref, get_json_object(payload, '$.description') AS description FROM s3t.github.events_json WHERE type = 'CreateEvent' AND get_json_object(payload, '$.ref_type') = 'branch'",
             "SELECT id, variant_get(payload, '$.action', 'string') AS action, variant_get(payload, '$.ref_type', 'string') AS ref_type, variant_get(payload, '$.ref', 'string') AS ref, variant_get(payload, '$.description', 'string') AS description FROM s3t.github.events_variant WHERE type = 'CreateEvent' AND variant_get(payload, '$.ref_type', 'string') = 'branch'"},

            {"Case7_Baseline_No_Payload",
             "SELECT type, COUNT(*) AS cnt, COUNT(DISTINCT get_json_object(actor, '$.login')) AS unique_actors FROM s3t.github.events_json GROUP BY type ORDER BY cnt DESC",
             "SELECT type, COUNT(*) AS cnt, COUNT(DISTINCT get_json_object(actor, '$.login')) AS unique_actors FROM s3t.github.events_variant GROUP BY type ORDER BY cnt DESC"}
        };

        System.out.println("========================================");
        System.out.println("  GitHub Events Query Benchmark");
        System.out.println("  Warmup: " + WARMUP_RUNS + ", Bench: " + BENCH_RUNS);
        System.out.println("========================================");

        List<String> results = new ArrayList<>();

        for (String[] c : cases) {
            String name = c[0];
            System.out.println("\n>>> " + name + " <<<");

            long jsonMs = benchmark(spark, name + "_JSON", c[1]);
            long variantMs = benchmark(spark, name + "_VARIANT", c[2]);

            double speedup = variantMs > 0 ? (double) jsonMs / variantMs : 0;
            String line = String.format("| %s | %,d | %,d | %.2fx |", name, jsonMs, variantMs, speedup);
            results.add(line);
            System.out.println("RESULT: " + line);
        }

        System.out.println("\n========================================");
        System.out.println("  FINAL RESULTS");
        System.out.println("========================================");
        System.out.println("| Case | JSON (ms) | Variant (ms) | Speedup |");
        System.out.println("| ---- | --------- | ------------ | ------- |");
        for (String r : results) {
            System.out.println(r);
        }
        System.out.println("========================================");

        spark.stop();
    }

    static long benchmark(SparkSession spark, String label, String sql) {
        for (int i = 0; i < WARMUP_RUNS; i++) {
            try {
                spark.sql(sql).collect();
            } catch (Exception e) {
                System.out.println("  " + label + " warmup FAILED: " + e.getMessage());
                return -1;
            }
        }

        long[] times = new long[BENCH_RUNS];
        for (int i = 0; i < BENCH_RUNS; i++) {
            long start = System.currentTimeMillis();
            try {
                spark.sql(sql).collect();
            } catch (Exception e) {
                System.out.println("  " + label + " run " + (i+1) + " FAILED: " + e.getMessage());
                return -1;
            }
            times[i] = System.currentTimeMillis() - start;
            System.out.println("  " + label + " run " + (i+1) + ": " + times[i] + " ms");
        }

        Arrays.sort(times);
        long median = times[BENCH_RUNS / 2];
        System.out.println("  " + label + " MEDIAN: " + median + " ms");
        return median;
    }
}

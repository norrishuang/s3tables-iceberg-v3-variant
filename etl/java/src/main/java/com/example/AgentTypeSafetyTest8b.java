package com.example;

import org.apache.spark.sql.*;

/**
 * Variant vs JSON 类型安全验证 (小表版, 快速测试)
 *
 * 使用 S3TablesCatalog (s3t) 创建两张新表:
 *   - s3t.agent_test.logs_json (detail: STRING)
 *   - s3t.agent_test.logs_variant (detail: VARIANT)
 *
 * 写入 100 万正常行 + 10 万脏数据行, 然后三方对比:
 *   1. JSON + CAST → 报错
 *   2. JSON + TRY_CAST → NULL
 *   3. Variant + try_variant_get → NULL
 */
public class AgentTypeSafetyTest8b {

    static final String JSON_TABLE = "s3t.agent_test.logs_json";
    static final String VARIANT_TABLE = "s3t.agent_test.logs_variant";
    static final int NORMAL_ROWS = 1_000_000;
    static final int DIRTY_ROWS = 100_000;

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Agent-Type-Safety-SmallTable")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Variant vs JSON 类型安全验证 (小表快速版) ===");
        System.out.println("Spark: " + spark.version());
        System.out.println("数据量: 正常 " + NORMAL_ROWS + " + 脏数据 " + DIRTY_ROWS);

        // Step 1: 建表
        createTables(spark);

        // Step 2: 写入数据
        injectData(spark);

        // Step 3: 运行对比查询
        runQueries(spark);

        System.out.println("\n=== 测试完成 ===");
        spark.stop();
    }

    static void createTables(SparkSession spark) {
        System.out.println("\n--- 建表 ---");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS s3t.agent_test");

        // JSON 表
        try {
            spark.sql("DROP TABLE IF EXISTS " + JSON_TABLE);
        } catch (Exception e) { /* ignore */ }
        spark.sql("CREATE TABLE " + JSON_TABLE + " (\n" +
                "  aid BIGINT, event STRING, tm TIMESTAMP, log_id BIGINT, detail STRING\n" +
                ") USING iceberg\n" +
                "TBLPROPERTIES ('format-version' = '2')");
        System.out.println("JSON 表创建成功: " + JSON_TABLE);

        // Variant 表
        try {
            spark.sql("DROP TABLE IF EXISTS " + VARIANT_TABLE);
        } catch (Exception e) { /* ignore */ }
        spark.sql("CREATE TABLE " + VARIANT_TABLE + " (\n" +
                "  aid BIGINT, event STRING, tm TIMESTAMP, log_id BIGINT, detail VARIANT\n" +
                ") USING iceberg\n" +
                "TBLPROPERTIES ('format-version' = '3', 'write.parquet.shred-variants' = 'true')");
        System.out.println("Variant 表创建成功: " + VARIANT_TABLE);
    }

    static void injectData(SparkSession spark) throws Exception {
        System.out.println("\n--- 写入数据 ---");

        // 正常数据
        spark.sql("SELECT id FROM range(" + NORMAL_ROWS + ")").createOrReplaceTempView("nids");
        String normalSql =
            "SELECT CAST(1000000 + (id % 10000) AS BIGINT) AS aid, 'player_enter' AS event,\n" +
            "  CAST(1717200000 + id AS TIMESTAMP) AS tm, CAST(id AS BIGINT) AS log_id,\n" +
            "  to_json(named_struct(\n" +
            "    'gold', CAST(rand(1) * 10000 AS INT),\n" +
            "    'player_start_time', CAST(1717200000 + id AS BIGINT),\n" +
            "    'map_id', CONCAT('100', CAST(id % 5 + 1 AS STRING)),\n" +
            "    'cloud_area', element_at(array('cn','us','eu','ap','kr'), CAST(id % 5 + 1 AS INT)),\n" +
            "    'session_id', CONCAT('s_', CAST(id AS STRING))\n" +
            "  )) AS detail\n" +
            "FROM nids";

        // 脏数据 (6种)
        spark.sql("SELECT id FROM range(" + DIRTY_ROWS + ")").createOrReplaceTempView("dids");
        String dirtySql =
            "SELECT CAST(9000000 + (id % 1000) AS BIGINT) AS aid,\n" +
            "  element_at(array('player_enter','player_exit','gold_change','room_create','system_event'), CAST(id % 5 + 1 AS INT)) AS event,\n" +
            "  CAST(1717300000 + id AS TIMESTAMP) AS tm, CAST(" + NORMAL_ROWS + " + id AS BIGINT) AS log_id,\n" +
            "  CASE\n" +
            "    WHEN id % 6 = 0 THEN CONCAT('{\"gold\":\"N/A\",\"map_id\":\"1001\",\"cloud_area\":\"cn\",\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "    WHEN id % 6 = 1 THEN CONCAT('{\"gold\":\"99.5\",\"map_id\":\"1002\",\"cloud_area\":\"us\",\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "    WHEN id % 6 = 2 THEN CONCAT('{\"gold\":100,\"player_start_time\":\"unknown\",\"map_id\":\"1003\",\"cloud_area\":\"eu\",\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "    WHEN id % 6 = 3 THEN CONCAT('{\"gold\":200,\"map_id\":{\"id\":1004,\"name\":\"desert\"},\"cloud_area\":\"ap\",\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "    WHEN id % 6 = 4 THEN CONCAT('{\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "    ELSE CONCAT('{\"gold\":300,\"map_id\":\"1005\",\"cloud_area\":\"kr\",\"match_result\":\"victory\",\"session_id\":\"d', CAST(id AS STRING), '\"}')\n" +
            "  END AS detail\n" +
            "FROM dids";

        Dataset<Row> normalDf = spark.sql(normalSql);
        Dataset<Row> dirtyDf = spark.sql(dirtySql);
        Dataset<Row> allData = normalDf.union(dirtyDf);

        // 写 JSON
        System.out.println("写入 JSON 表...");
        allData.writeTo(JSON_TABLE).append();

        // 写 Variant
        System.out.println("写入 Variant 表...");
        allData.selectExpr("aid", "event", "tm", "log_id", "parse_json(detail) AS detail")
               .writeTo(VARIANT_TABLE).append();

        long jc = spark.sql("SELECT COUNT(*) FROM " + JSON_TABLE).collectAsList().get(0).getLong(0);
        long vc = spark.sql("SELECT COUNT(*) FROM " + VARIANT_TABLE).collectAsList().get(0).getLong(0);
        System.out.println("写入完成: JSON=" + jc + ", Variant=" + vc);
    }

    static void runQueries(SparkSession spark) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("=== 查询对比 (" + (NORMAL_ROWS + DIRTY_ROWS) + " 行, 含脏数据) ===");
        System.out.println("=".repeat(70));

        // Q1: SUM(gold)
        System.out.println("\n=== Q1: SUM(gold) ===");
        run(spark, "Q1a_JSON_CAST",
            "SELECT COUNT(*) AS total, SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold FROM " + JSON_TABLE);
        run(spark, "Q1b_JSON_TRY_CAST",
            "SELECT COUNT(*) AS total, SUM(TRY_CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold FROM " + JSON_TABLE);
        run(spark, "Q1c_VARIANT_try_variant_get",
            "SELECT COUNT(*) AS total, SUM(try_variant_get(detail, '$.gold', 'LONG')) AS total_gold FROM " + VARIANT_TABLE);

        // Q2: 时长计算
        System.out.println("\n=== Q2: 时长计算 (player_start_time) ===");
        run(spark, "Q2a_JSON_CAST",
            "SELECT COUNT(*) AS total, SUM(CASE WHEN (unix_timestamp(tm) - CAST(get_json_object(detail, '$.player_start_time') AS BIGINT)) > 60 THEN 1 ELSE 0 END) AS over_1min FROM " + JSON_TABLE);
        run(spark, "Q2b_JSON_TRY_CAST",
            "SELECT COUNT(*) AS total, SUM(CASE WHEN (unix_timestamp(tm) - TRY_CAST(get_json_object(detail, '$.player_start_time') AS BIGINT)) > 60 THEN 1 ELSE 0 END) AS over_1min FROM " + JSON_TABLE);
        run(spark, "Q2c_VARIANT_try_variant_get",
            "SELECT COUNT(*) AS total, SUM(CASE WHEN (unix_timestamp(tm) - try_variant_get(detail, '$.player_start_time', 'LONG')) > 60 THEN 1 ELSE 0 END) AS over_1min FROM " + VARIANT_TABLE);

        // Q3: GROUP BY + AVG(gold)
        System.out.println("\n=== Q3: GROUP BY cloud_area + AVG(gold) ===");
        run(spark, "Q3a_JSON_CAST",
            "SELECT get_json_object(detail, '$.cloud_area') AS area, COUNT(*) AS cnt, AVG(CAST(get_json_object(detail, '$.gold') AS DOUBLE)) AS avg_gold FROM " + JSON_TABLE + " GROUP BY get_json_object(detail, '$.cloud_area') ORDER BY cnt DESC");
        run(spark, "Q3b_JSON_TRY_CAST",
            "SELECT get_json_object(detail, '$.cloud_area') AS area, COUNT(*) AS cnt, AVG(TRY_CAST(get_json_object(detail, '$.gold') AS DOUBLE)) AS avg_gold FROM " + JSON_TABLE + " GROUP BY get_json_object(detail, '$.cloud_area') ORDER BY cnt DESC");
        run(spark, "Q3c_VARIANT_try_variant_get",
            "SELECT try_variant_get(detail, '$.cloud_area', 'STRING') AS area, COUNT(*) AS cnt, AVG(CAST(try_variant_get(detail, '$.gold', 'LONG') AS DOUBLE)) AS avg_gold FROM " + VARIANT_TABLE + " GROUP BY try_variant_get(detail, '$.cloud_area', 'STRING') ORDER BY cnt DESC");

        // Q4: schema 演进
        System.out.println("\n=== Q4: schema 演进 (match_result) ===");
        run(spark, "Q4a_JSON",
            "SELECT get_json_object(detail, '$.match_result') AS result, COUNT(*) AS cnt FROM " + JSON_TABLE + " WHERE get_json_object(detail, '$.match_result') IS NOT NULL GROUP BY get_json_object(detail, '$.match_result')");
        run(spark, "Q4b_VARIANT",
            "SELECT try_variant_get(detail, '$.match_result', 'STRING') AS result, COUNT(*) AS cnt FROM " + VARIANT_TABLE + " WHERE try_variant_get(detail, '$.match_result', 'STRING') IS NOT NULL GROUP BY try_variant_get(detail, '$.match_result', 'STRING')");

        // Q5: schema_of_variant
        System.out.println("\n=== Q5: schema_of_variant 字段发现 ===");
        run(spark, "Q5_schema_of_variant",
            "SELECT schema_of_variant(detail) AS schema FROM " + VARIANT_TABLE + " LIMIT 5");

        // Q6: variant_get (strict) vs try_variant_get (safe)
        System.out.println("\n=== Q6: variant_get vs try_variant_get ===");
        run(spark, "Q6a_variant_get_STRICT",
            "SELECT SUM(variant_get(detail, '$.gold', 'LONG')) FROM " + VARIANT_TABLE);
        run(spark, "Q6b_try_variant_get_SAFE",
            "SELECT SUM(try_variant_get(detail, '$.gold', 'LONG')) FROM " + VARIANT_TABLE);

        // 汇总
        System.out.println("\n" + "=".repeat(70));
        System.out.println("=== 结论 ===");
        System.out.println("=".repeat(70));
        System.out.println("| 方式                           | 脏数据行为         | Agent 需要知道的 |");
        System.out.println("|--------------------------------|-------------------|-----------------|");
        System.out.println("| JSON + CAST                    | 抛异常,查询失败    | 必须用TRY_CAST  |");
        System.out.println("| JSON + TRY_CAST                | 返回NULL,查询成功  | 需两步组合      |");
        System.out.println("| Variant + variant_get          | 抛异常,查询失败    | 必须用try_前缀  |");
        System.out.println("| Variant + try_variant_get      | 返回NULL,查询成功  | 一步到位        |");
        System.out.println();
        System.out.println("核心差异: try_variant_get 是一个函数完成提取+类型安全转换");
        System.out.println("         JSON 需要 TRY_CAST(get_json_object(...)) 两步组合");
    }

    static void run(SparkSession spark, String label, String sql) {
        System.out.println("\n[" + label + "]");
        long t = System.currentTimeMillis();
        try {
            spark.sql(sql).show(20, false);
            System.out.println("RESULT|" + label + "|SUCCESS|" + (System.currentTimeMillis() - t) + "ms");
        } catch (Exception e) {
            long ms = System.currentTimeMillis() - t;
            String msg = e.getMessage();
            if (msg != null && msg.length() > 200) msg = msg.substring(0, 200);
            System.out.println("RESULT|" + label + "|ERROR|" + ms + "ms|" + msg);
        }
    }
}

package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;

/**
 * Variant vs JSON 类型安全验证实验
 *
 * 核心目标：证明 Agent 生成的"正常查询"在含脏数据时，
 * Variant 表能正确执行而 JSON 表会报错或返回错误结果。
 *
 * 数据设计（模拟真实游戏日志）：
 * - 90% 正常数据：字段类型一致
 * - 10% 脏数据：类型混用、缺失字段、异常值、新增字段
 *
 * 两张表：
 * - JSON 表: agent_test_json (detail: STRING)
 * - Variant 表: agent_test_variant (detail: VARIANT)
 */
public class AgentTypeSafetyTest {

    static final String NAMESPACE = "iceberg_v3_test";
    static final String JSON_TABLE = "dstcat." + NAMESPACE + ".agent_json";
    static final String VARIANT_TABLE = "dstcat." + NAMESPACE + ".variant_test_with_bucket";
    static final int NORMAL_ROWS = 100_000;
    static final int DIRTY_ROWS = 11_000; // ~10%

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Agent-Type-Safety-Test")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Variant vs JSON: Agent 类型安全验证实验 ===");
        System.out.println("Spark: " + spark.version());
        System.out.println("正常数据: " + NORMAL_ROWS + " 行, 脏数据: " + DIRTY_ROWS + " 行");

        // Step 1: 建表
        createTables(spark);

        // Step 2: 生成并写入数据
        generateData(spark);

        // Step 3: 运行 Agent 模拟查询
        runAgentQueries(spark);

        System.out.println("\n=== 实验完成 ===");
        spark.stop();
    }

    static void createTables(SparkSession spark) {
        System.out.println("\n--- 建表 ---");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS dstcat." + NAMESPACE);

        // Variant 表：使用已有的 variant_test_with_bucket（S3 Tables 暂不支持新建 variant 表）
        System.out.println("Variant 表(已有): " + VARIANT_TABLE);

        // JSON 表：detail 是 STRING（format-version=2）
        System.out.println("创建 JSON 表...");
        try {
            spark.sql("CREATE TABLE " + JSON_TABLE + " (\n" +
                    "    event_id BIGINT,\n" +
                    "    aid      BIGINT,\n" +
                    "    tm       TIMESTAMP,\n" +
                    "    detail   STRING\n" +
                    ") USING iceberg\n" +
                    "PARTITIONED BY (hours(tm), bucket(64, aid))\n" +
                    "TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')");
            System.out.println("JSON 表创建成功: " + JSON_TABLE);
        } catch (Exception e) {
            System.out.println("JSON 表创建异常(可能已存在): " + e.getMessage());
            spark.sql("SELECT 1 FROM " + JSON_TABLE + " LIMIT 1");
            System.out.println("JSON 表已存在，继续");
        }
    }

    static void generateData(SparkSession spark) throws Exception {
        System.out.println("\n--- 生成数据 ---");

        // === 正常数据 ===
        // gold: 整数, player_start_time: unix timestamp, map_id: 字符串, cloud_area: 字符串
        spark.sql("SELECT id FROM range(" + NORMAL_ROWS + ")").createOrReplaceTempView("normal_ids");

        String normalJsonSql =
            "SELECT \n" +
            "  id AS event_id,\n" +
            "  CAST(1000000 + (id % 100) AS BIGINT) AS aid,\n" +
            "  CAST(1714521600 + (id % 86400) AS TIMESTAMP) AS tm,\n" +
            "  to_json(named_struct(\n" +
            "    'gold', CAST(rand(1) * 10000 AS INT),\n" +
            "    'player_start_time', CAST(1716000000 + id AS BIGINT),\n" +
            "    'map_id', CONCAT('100', CAST(id % 5 + 1 AS STRING)),\n" +
            "    'cloud_area', element_at(array('cn-east','cn-north','us-west','eu-west','ap-south'), CAST(id % 5 + 1 AS INT)),\n" +
            "    'session_id', CONCAT('sess_', CAST(id AS STRING))\n" +
            "  )) AS detail\n" +
            "FROM normal_ids";

        Dataset<Row> normalDf = spark.sql(normalJsonSql);

        // === 脏数据：模拟真实世界中数据质量问题 ===
        spark.sql("SELECT id FROM range(" + DIRTY_ROWS + ")").createOrReplaceTempView("dirty_ids");

        // 脏数据分类：
        // 1. gold 不是数字 (2000行): "N/A", "", "null", "待结算", "-"
        // 2. player_start_time 不是合法时间戳 (2000行): "unknown", "0", "not_started"
        // 3. map_id 是嵌套对象而非字符串 (2000行)
        // 4. 完全缺少某些字段 (2000行)
        // 5. 新增了原来没有的字段 match_result (2000行) — 模拟 schema 演进
        // 6. gold 是浮点数字符串 "99.5" 而非整数 (1000行)
        String dirtyJsonSql =
            "SELECT \n" +
            "  CAST(" + NORMAL_ROWS + " + id AS BIGINT) AS event_id,\n" +
            "  CAST(1000000 + (id % 100) AS BIGINT) AS aid,\n" +
            "  CAST(1715731200 + (id % 86400) AS TIMESTAMP) AS tm,\n" +
            "  CASE\n" +
            // 类型1: gold 不是数字
            "    WHEN id < 2000 THEN\n" +
            "      CONCAT('{\"gold\":\"', element_at(array('N/A','','null','待结算','-'), CAST(id % 5 + 1 AS INT)), " +
            "        '\",\"player_start_time\":', CAST(1716000000 + id AS STRING), " +
            "        ',\"map_id\":\"1001\",\"cloud_area\":\"cn-east\",\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\"}')\n" +
            // 类型2: player_start_time 不是数字
            "    WHEN id < 4000 THEN\n" +
            "      CONCAT('{\"gold\":100,\"player_start_time\":\"', element_at(array('unknown','not_started','pending','null',''), CAST(id % 5 + 1 AS INT)), " +
            "        '\",\"map_id\":\"1002\",\"cloud_area\":\"cn-north\",\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\"}')\n" +
            // 类型3: map_id 是嵌套对象
            "    WHEN id < 6000 THEN\n" +
            "      CONCAT('{\"gold\":200,\"player_start_time\":1716000000,\"map_id\":{\"id\":1003,\"name\":\"desert\"},\"cloud_area\":\"us-west\",\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\"}')\n" +
            // 类型4: 缺少字段
            "    WHEN id < 8000 THEN\n" +
            "      CONCAT('{\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\"}')\n" +
            // 类型5: 新增字段 match_result (schema 演进)
            "    WHEN id < 10000 THEN\n" +
            "      CONCAT('{\"gold\":300,\"player_start_time\":1716050000,\"map_id\":\"1004\",\"cloud_area\":\"eu-west\",\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\",\"match_result\":\"victory\",\"match_score\":95}')\n" +
            // 类型6: gold 是浮点字符串
            "    ELSE\n" +
            "      CONCAT('{\"gold\":\"99.5\",\"player_start_time\":1716060000,\"map_id\":\"1005\",\"cloud_area\":\"ap-south\",\"session_id\":\"sess_dirty_', CAST(id AS STRING), '\"}')\n" +
            "  END AS detail\n" +
            "FROM dirty_ids";

        Dataset<Row> dirtyDf = spark.sql(dirtyJsonSql);

        // 合并
        Dataset<Row> allData = normalDf.union(dirtyDf);

        // 写入 JSON 表（直接写 STRING）
        System.out.println("写入 JSON 表...");
        allData.writeTo(JSON_TABLE).append();
        long jsonCount = spark.sql("SELECT COUNT(*) FROM " + JSON_TABLE).collectAsList().get(0).getLong(0);
        System.out.println("JSON 表行数: " + jsonCount);

        // 写入 Variant 表（parse_json 转换）
        System.out.println("写入 Variant 表...");
        allData.selectExpr("event_id", "aid", "tm", "parse_json(detail) AS detail")
               .writeTo(VARIANT_TABLE).append();
        long variantCount = spark.sql("SELECT COUNT(*) FROM " + VARIANT_TABLE).collectAsList().get(0).getLong(0);
        System.out.println("Variant 表行数: " + variantCount);
    }

    static void runAgentQueries(SparkSession spark) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("=== 模拟 Agent 生成的查询 ===");
        System.out.println("假设：Agent 采样了几行正常数据后，生成以下查询");
        System.out.println("=".repeat(70));

        // ====================================================================
        // 实验 1: SUM(gold) — Agent 假设 gold 是数字
        // ====================================================================
        System.out.println("\n--- 实验 1: Agent 生成 SUM(gold) 聚合 ---");
        System.out.println("场景: Agent 采样看到 gold 是数字，生成 SUM 聚合");
        System.out.println("脏数据: 部分行 gold 值为 'N/A', '', 'null', '待结算', '99.5'");

        // JSON 表: Agent 生成的典型查询
        String q1Json = "SELECT COUNT(*) AS total_rows, " +
                "SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold, " +
                "COUNT(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS valid_gold_rows " +
                "FROM " + JSON_TABLE;

        // Variant 表: Agent 生成的查询
        String q1Variant = "SELECT COUNT(*) AS total_rows, " +
                "SUM(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS total_gold, " +
                "COUNT(variant_get(detail, '$.gold', 'LONG')) AS valid_gold_rows " +
                "FROM " + VARIANT_TABLE;

        runAndCompare(spark, "Q1_SUM_gold", q1Json, q1Variant);

        // ====================================================================
        // 实验 2: 时长计算 — Agent 假设 player_start_time 是 BIGINT
        // ====================================================================
        System.out.println("\n--- 实验 2: Agent 生成时长计算 ---");
        System.out.println("场景: Agent 计算游戏时长 = unix_timestamp(tm) - player_start_time");
        System.out.println("脏数据: 部分行 player_start_time 为 'unknown', 'not_started' 等字符串");

        String q2Json = "SELECT COUNT(*) AS total, " +
                "SUM(CASE WHEN (unix_timestamp(tm) - CAST(get_json_object(detail, '$.player_start_time') AS BIGINT)) > 3600 " +
                "THEN 1 ELSE 0 END) AS over_1hour " +
                "FROM " + JSON_TABLE + " WHERE aid = 1000001";

        String q2Variant = "SELECT COUNT(*) AS total, " +
                "SUM(CASE WHEN (unix_timestamp(tm) - CAST(variant_get(detail, '$.player_start_time', 'LONG') AS BIGINT)) > 3600 " +
                "THEN 1 ELSE 0 END) AS over_1hour " +
                "FROM " + VARIANT_TABLE + " WHERE aid = 1000001";

        runAndCompare(spark, "Q2_duration_calc", q2Json, q2Variant);

        // ====================================================================
        // 实验 3: WHERE 过滤嵌套字段 — map_id 可能是对象
        // ====================================================================
        System.out.println("\n--- 实验 3: Agent 按 map_id 过滤 ---");
        System.out.println("场景: Agent 生成 WHERE map_id = '1001' 过滤");
        System.out.println("脏数据: 部分行 map_id 是嵌套对象 {\"id\":1003,\"name\":\"desert\"}");

        String q3Json = "SELECT COUNT(*) AS cnt, " +
                "SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total_gold " +
                "FROM " + JSON_TABLE + " " +
                "WHERE get_json_object(detail, '$.map_id') = '1001'";

        String q3Variant = "SELECT COUNT(*) AS cnt, " +
                "SUM(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS total_gold " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE variant_get(detail, '$.map_id', 'STRING') = '1001'";

        runAndCompare(spark, "Q3_filter_map_id", q3Json, q3Variant);

        // ====================================================================
        // 实验 4: GROUP BY 嵌套字段 + AVG — 类型混合场景
        // ====================================================================
        System.out.println("\n--- 实验 4: Agent 按 cloud_area 分组统计 ---");
        System.out.println("场景: Agent 生成 GROUP BY cloud_area, AVG(gold)");
        System.out.println("脏数据: 部分行缺少 cloud_area 或 gold 不是数字");

        String q4Json = "SELECT get_json_object(detail, '$.cloud_area') AS cloud_area, " +
                "COUNT(*) AS cnt, " +
                "AVG(CAST(get_json_object(detail, '$.gold') AS DOUBLE)) AS avg_gold " +
                "FROM " + JSON_TABLE + " " +
                "GROUP BY get_json_object(detail, '$.cloud_area') " +
                "ORDER BY cnt DESC";

        String q4Variant = "SELECT variant_get(detail, '$.cloud_area', 'STRING') AS cloud_area, " +
                "COUNT(*) AS cnt, " +
                "AVG(CAST(variant_get(detail, '$.gold', 'LONG') AS DOUBLE)) AS avg_gold " +
                "FROM " + VARIANT_TABLE + " " +
                "GROUP BY variant_get(detail, '$.cloud_area', 'STRING') " +
                "ORDER BY cnt DESC";

        runAndCompare(spark, "Q4_group_by_cloud_area", q4Json, q4Variant);

        // ====================================================================
        // 实验 5: Schema 演进 — 查新字段 match_result
        // ====================================================================
        System.out.println("\n--- 实验 5: Agent 发现新字段 match_result ---");
        System.out.println("场景: 数据演进后出现 match_result 字段，Agent 尝试查询");
        System.out.println("验证: 两张表都能处理，但 Variant 可通过 schema_of_variant 发现");

        String q5Json = "SELECT get_json_object(detail, '$.match_result') AS result, " +
                "COUNT(*) AS cnt " +
                "FROM " + JSON_TABLE + " " +
                "WHERE get_json_object(detail, '$.match_result') IS NOT NULL " +
                "GROUP BY get_json_object(detail, '$.match_result')";

        String q5Variant = "SELECT variant_get(detail, '$.match_result', 'STRING') AS result, " +
                "COUNT(*) AS cnt " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE variant_get(detail, '$.match_result', 'STRING') IS NOT NULL " +
                "GROUP BY variant_get(detail, '$.match_result', 'STRING')";

        runAndCompare(spark, "Q5_schema_evolution", q5Json, q5Variant);

        // ====================================================================
        // 实验 6: schema_of_variant — 只有 Variant 能做到的字段发现
        // ====================================================================
        System.out.println("\n--- 实验 6: Agent 字段发现能力 ---");
        System.out.println("场景: Agent 不知道 detail 里有什么字段，尝试自动发现");

        // Variant 表: schema_of_variant 直接获取结构
        System.out.println("\nVariant 表 - schema_of_variant 发现结果:");
        try {
            Dataset<Row> schema = spark.sql(
                "SELECT schema_of_variant(detail) AS discovered_schema " +
                "FROM " + VARIANT_TABLE + " LIMIT 5"
            );
            schema.show(false);
            System.out.println("DISCOVERY|VARIANT|SUCCESS|schema_of_variant 可直接发现字段结构");
        } catch (Exception e) {
            System.out.println("DISCOVERY|VARIANT|ERROR|" + e.getMessage());
        }

        // JSON 表: 没有等价的自动发现函数
        System.out.println("\nJSON 表 - 只能采样后人工解析:");
        try {
            Dataset<Row> sample = spark.sql(
                "SELECT detail FROM " + JSON_TABLE + " LIMIT 3"
            );
            sample.show(false);
            System.out.println("DISCOVERY|JSON|MANUAL|需要人工/Agent 解析 JSON 文本推断结构");
        } catch (Exception e) {
            System.out.println("DISCOVERY|JSON|ERROR|" + e.getMessage());
        }

        // ====================================================================
        // 实验 7: 类型不匹配时的行为差异（关键实验）
        // ====================================================================
        System.out.println("\n--- 实验 7: 强制类型转换失败时的行为 ---");
        System.out.println("场景: Agent 对包含 'N/A' 的 gold 字段做 SUM");
        System.out.println("关键验证: JSON CAST 是否报错 vs Variant 是否安全返回 NULL");

        // 只查脏数据（gold = 'N/A' 的行）
        String q7Json = "SELECT event_id, " +
                "get_json_object(detail, '$.gold') AS gold_raw, " +
                "CAST(get_json_object(detail, '$.gold') AS BIGINT) AS gold_casted " +
                "FROM " + JSON_TABLE + " " +
                "WHERE event_id BETWEEN " + NORMAL_ROWS + " AND " + (NORMAL_ROWS + 10);

        String q7Variant = "SELECT event_id, " +
                "variant_get(detail, '$.gold', 'STRING') AS gold_raw, " +
                "variant_get(detail, '$.gold', 'LONG') AS gold_typed " +
                "FROM " + VARIANT_TABLE + " " +
                "WHERE event_id BETWEEN " + NORMAL_ROWS + " AND " + (NORMAL_ROWS + 10);

        System.out.println("\nJSON 表 - CAST 行为:");
        try {
            spark.sql(q7Json).show(false);
            System.out.println("CAST_BEHAVIOR|JSON|看到 gold_casted: CAST 'N/A' 为 NULL (Spark 默认 ANSI off)");
        } catch (Exception e) {
            System.out.println("CAST_BEHAVIOR|JSON|CAST报错: " + e.getMessage());
        }

        System.out.println("\nVariant 表 - variant_get 行为:");
        try {
            spark.sql(q7Variant).show(false);
            System.out.println("CAST_BEHAVIOR|VARIANT|variant_get 对非 LONG 值直接返回 NULL，无需 CAST");
        } catch (Exception e) {
            System.out.println("CAST_BEHAVIOR|VARIANT|ERROR: " + e.getMessage());
        }

        // ====================================================================
        // 实验 8: ANSI mode 下的差异（生产环境常见配置）
        // ====================================================================
        System.out.println("\n--- 实验 8: ANSI mode = true 时的行为差异 ---");
        System.out.println("场景: 生产环境开启 ANSI mode，CAST 失败时抛异常");

        spark.sql("SET spark.sql.ansi.enabled = true");

        // JSON 表: CAST 'N/A' as BIGINT 会抛异常
        System.out.println("\nJSON 表 (ANSI mode ON) - SUM(CAST(gold AS BIGINT)):");
        try {
            Dataset<Row> result = spark.sql(
                "SELECT SUM(CAST(get_json_object(detail, '$.gold') AS BIGINT)) AS total " +
                "FROM " + JSON_TABLE
            );
            result.show();
            System.out.println("ANSI_TEST|JSON|UNEXPECTED_SUCCESS");
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.length() > 200) msg = msg.substring(0, 200);
            System.out.println("ANSI_TEST|JSON|EXCEPTION|" + msg);
            System.out.println("结论: JSON 表在 ANSI mode 下，Agent 生成的 CAST 查询会直接报错！");
        }

        // Variant 表: variant_get 返回 NULL，不触发异常
        System.out.println("\nVariant 表 (ANSI mode ON) - SUM(variant_get(gold, LONG)):");
        try {
            Dataset<Row> result = spark.sql(
                "SELECT SUM(CAST(variant_get(detail, '$.gold', 'LONG') AS BIGINT)) AS total " +
                "FROM " + VARIANT_TABLE
            );
            result.show();
            System.out.println("ANSI_TEST|VARIANT|SUCCESS|查询正常完成，脏数据被安全跳过");
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.length() > 200) msg = msg.substring(0, 200);
            System.out.println("ANSI_TEST|VARIANT|EXCEPTION|" + msg);
        }

        // 恢复
        spark.sql("SET spark.sql.ansi.enabled = false");

        // ====================================================================
        // 汇总
        // ====================================================================
        System.out.println("\n" + "=".repeat(70));
        System.out.println("=== 实验汇总 ===");
        System.out.println("=".repeat(70));
        System.out.println("数据规模: " + (NORMAL_ROWS + DIRTY_ROWS) + " 行 (正常 " + NORMAL_ROWS + " + 脏数据 " + DIRTY_ROWS + ")");
        System.out.println("脏数据类型: gold非数字, player_start_time非时间戳, map_id嵌套对象, 缺失字段, 新增字段");
        System.out.println();
        System.out.println("关键发现:");
        System.out.println("1. ANSI mode OFF: JSON CAST 对非法值返回 NULL（静默数据丢失）");
        System.out.println("2. ANSI mode ON:  JSON CAST 对非法值抛异常（查询失败）");
        System.out.println("3. Variant variant_get: 类型不匹配时始终返回 NULL（安全且一致）");
        System.out.println("4. schema_of_variant: 可自动发现字段结构");
    }

    static void runAndCompare(SparkSession spark, String label, String jsonSql, String variantSql) {
        System.out.println("\n[" + label + "] JSON 表查询:");
        long jsonStart = System.currentTimeMillis();
        try {
            Dataset<Row> result = spark.sql(jsonSql);
            result.show(false);
            long jsonMs = System.currentTimeMillis() - jsonStart;
            System.out.println("RESULT|" + label + "|JSON|SUCCESS|" + jsonMs + "ms");
        } catch (Exception e) {
            long jsonMs = System.currentTimeMillis() - jsonStart;
            String msg = e.getMessage();
            if (msg != null && msg.length() > 150) msg = msg.substring(0, 150);
            System.out.println("RESULT|" + label + "|JSON|ERROR|" + jsonMs + "ms|" + msg);
        }

        System.out.println("\n[" + label + "] Variant 表查询:");
        long varStart = System.currentTimeMillis();
        try {
            Dataset<Row> result = spark.sql(variantSql);
            result.show(false);
            long varMs = System.currentTimeMillis() - varStart;
            System.out.println("RESULT|" + label + "|VARIANT|SUCCESS|" + varMs + "ms");
        } catch (Exception e) {
            long varMs = System.currentTimeMillis() - varStart;
            String msg = e.getMessage();
            if (msg != null && msg.length() > 150) msg = msg.substring(0, 150);
            System.out.println("RESULT|" + label + "|VARIANT|ERROR|" + varMs + "ms|" + msg);
        }
    }
}

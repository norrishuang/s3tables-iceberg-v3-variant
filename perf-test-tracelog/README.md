# Agent Trace Log 性能测试 — Variant vs JSON

## 目标

对比 **Iceberg V3 VARIANT（含 Shredding）** 与 **传统 JSON 字符串存储** 在 Agent Trace Log（OTel spans）数据上的查询性能差异。

数据源为 Kafka Streaming 实时写入的 `s3tablesbucket.tracelog.otel_spans` 表（VARIANT + Shredding），本测试额外创建一张 schema 相同但使用 STRING 类型存储 JSON 的对照表。

## 表结构

### 表 A（Variant + Shredding）— 已有表

```
s3tablesbucket.tracelog.otel_spans
```

| 列名 | 类型 | 说明 |
|------|------|------|
| trace_id | STRING | |
| span_id | STRING | |
| parent_span_id | STRING | |
| name | STRING | span 名称 |
| kind | INT | |
| start_time | TIMESTAMP | 分区键 days(start_time) |
| end_time | TIMESTAMP | |
| duration_ms | BIGINT | |
| status_code | INT | |
| service_name | STRING | |
| service_version | STRING | |
| deployment_env | STRING | |
| attributes | VARIANT | span attributes（扁平 map） |
| events | VARIANT | span events |
| resource_attributes | VARIANT | resource attributes |

### 表 B（JSON STRING 对照）— 测试建表

```
s3tablesbucket.tracelog.otel_spans_json
```

结构化列与表 A 完全相同，但：
- `attributes` → STRING（存储 JSON 字符串）
- `events` → STRING（存储 JSON 字符串）
- `resource_attributes` → STRING（存储 JSON 字符串）

分区策略相同：`PARTITIONED BY (days(start_time))`

## 测试方法

### 数据准备（SetupJob）

1. 从 `otel_spans`（VARIANT 表）读取全部数据
2. 将 VARIANT 列用 `to_json()` 转为 STRING
3. 写入 `otel_spans_json` 表
4. 确保两张表数据量一致、分区对齐

### 性能测试（PerfTest）

- 每条 SQL 执行 **3 次**，取平均值（wall-clock timing）
- 第 1 次包含冷启动开销（不单独 warm-up）
- 使用 `spark.sql(query).collect()` 触发完整执行
- 最终输出对比表格：Variant ms / JSON ms / Speedup（Variant 快于 JSON 的倍数）

## 测试用例

### Q1: 按 service_name 过滤 + 提取 attributes 中的字段

**场景**：查询某个服务最近的 token 消耗（高频运维查询）

```sql
-- Variant 版本
SELECT service_name,
       variant_get(attributes, '$.gen_ai.usage.input_tokens', 'long') AS input_tokens,
       variant_get(attributes, '$.gen_ai.usage.output_tokens', 'long') AS output_tokens,
       duration_ms
FROM {table}
WHERE service_name = '{service}'
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'

-- JSON 版本
SELECT service_name,
       get_json_object(attributes, '$.gen_ai.usage.input_tokens') AS input_tokens,
       get_json_object(attributes, '$.gen_ai.usage.output_tokens') AS output_tokens,
       duration_ms
FROM {table}
WHERE service_name = '{service}'
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
```

**验证重点**：Shredding 将高频 key（如 `gen_ai.usage.input_tokens`）物化为独立列，避免解析整个 JSON。

### Q2: 全表聚合 — attributes 中的数值字段统计

**场景**：全局 token 使用量统计

```sql
-- Variant 版本
SELECT service_name,
       COUNT(*) AS span_count,
       SUM(variant_get(attributes, '$.gen_ai.usage.input_tokens', 'long')) AS total_input_tokens,
       SUM(variant_get(attributes, '$.gen_ai.usage.output_tokens', 'long')) AS total_output_tokens,
       AVG(duration_ms) AS avg_duration
FROM {table}
WHERE start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
GROUP BY service_name
ORDER BY total_input_tokens DESC

-- JSON 版本
SELECT service_name,
       COUNT(*) AS span_count,
       SUM(CAST(get_json_object(attributes, '$.gen_ai.usage.input_tokens') AS BIGINT)) AS total_input_tokens,
       SUM(CAST(get_json_object(attributes, '$.gen_ai.usage.output_tokens') AS BIGINT)) AS total_output_tokens,
       AVG(duration_ms) AS avg_duration
FROM {table}
WHERE start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
GROUP BY service_name
ORDER BY total_input_tokens DESC
```

**验证重点**：Shredding 列直接参与聚合，跳过 JSON 解析 + CAST 开销。

### Q3: 多条件过滤 — attributes 中的字符串字段

**场景**：查找特定 AI model 的调用链

```sql
-- Variant 版本
SELECT trace_id, name, duration_ms,
       variant_get(attributes, '$.gen_ai.system', 'string') AS ai_system,
       variant_get(attributes, '$.gen_ai.request.model', 'string') AS model
FROM {table}
WHERE variant_get(attributes, '$.gen_ai.system', 'string') = 'anthropic'
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'

-- JSON 版本
SELECT trace_id, name, duration_ms,
       get_json_object(attributes, '$.gen_ai.system') AS ai_system,
       get_json_object(attributes, '$.gen_ai.request.model') AS model
FROM {table}
WHERE get_json_object(attributes, '$.gen_ai.system') = 'anthropic'
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
```

**验证重点**：WHERE 条件中对 VARIANT 字段的谓词下推（Shredding 列可直接过滤）。

### Q4: 嵌套聚合 — events 字段分析

**场景**：分析 span events 的数量分布

```sql
-- Variant 版本
SELECT name, service_name,
       variant_get(attributes, '$.gen_ai.usage.input_tokens', 'long') AS input_tokens,
       variant_get(attributes, '$.gen_ai.response.finish_reasons', 'string') AS finish_reason,
       duration_ms
FROM {table}
WHERE duration_ms > 5000
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
ORDER BY duration_ms DESC
LIMIT 100

-- JSON 版本
SELECT name, service_name,
       CAST(get_json_object(attributes, '$.gen_ai.usage.input_tokens') AS BIGINT) AS input_tokens,
       get_json_object(attributes, '$.gen_ai.response.finish_reasons') AS finish_reason,
       duration_ms
FROM {table}
WHERE duration_ms > 5000
  AND start_time >= timestamp '{start}' AND start_time < timestamp '{end}'
ORDER BY duration_ms DESC
LIMIT 100
```

**验证重点**：混合结构化列过滤（duration_ms）+ VARIANT 字段投影。

### Q5: Trace 关联查询 — 按 trace_id 重建调用链

**场景**：根据 trace_id 获取完整调用链

```sql
-- Variant 版本
SELECT span_id, parent_span_id, name, duration_ms,
       variant_get(attributes, '$.gen_ai.system', 'string') AS ai_system
FROM {table}
WHERE trace_id = '{trace_id}'
ORDER BY start_time

-- JSON 版本
SELECT span_id, parent_span_id, name, duration_ms,
       get_json_object(attributes, '$.gen_ai.system') AS ai_system
FROM {table}
WHERE trace_id = '{trace_id}'
ORDER BY start_time
```

**验证重点**：点查场景（单 trace_id），VARIANT 列的读取开销。

## 预期结论

| 场景 | 预期 Variant 优势 |
|------|-------------------|
| Q1 单字段提取 | 2-5x（Shredding 列直接读取 vs 全 JSON 解析） |
| Q2 聚合计算 | 3-10x（列式读取 + 统计下推） |
| Q3 WHERE 过滤 | 2-5x（谓词下推到 Shredding 列） |
| Q4 混合过滤+投影 | 2-4x |
| Q5 点查 | 1-2x（数据量小，差异较小） |

## 运行方式

### 构建

```bash
cd perf-test-tracelog
mvn clean package -DskipTests
aws s3 cp target/tracelog-perf-test-1.0.0-shaded.jar \
  s3://adap-prototype-812046859005/variant-shredding-test/jars/
```

### Step 1: 数据准备（建 JSON 对照表）

```bash
kubectl apply -f k8s/tracelog-perf-setup.yaml -n emr-eks-spark
```

### Step 2: 运行性能测试

```bash
kubectl apply -f k8s/tracelog-perf-test.yaml -n emr-eks-spark
```

### 查看日志

```bash
kubectl logs -f -n emr-eks-spark \
  $(kubectl get pod -n emr-eks-spark -l spark-app-name=tracelog-perf-test,spark-role=driver \
    -o jsonpath='{.items[0].metadata.name}')
```

## 技术栈

- Java 17 / Spark 4.1.2 / Iceberg 1.11
- S3 Tables REST Catalog (SigV4)
- EKS + Spark Operator (SparkApplication CRD)
- 镜像: `oss-spark:4.1.2-iceberg1.11-s3t`

# GitHub Events — Variant Shredding vs JSON 性能对比测试

## 测试目标

对比 Iceberg v3 VARIANT (shredding) 与传统 JSON STRING 在 **payload** 字段上的查询性能差异。

核心问题：对于结构高度异构的半结构化字段，Variant shredding 能带来多少查询性能提升？

---

## 数据集

| 项目 | 说明 |
|------|------|
| 来源 | [GH Archive](https://www.gharchive.org/) — GitHub 公开事件归档 |
| 格式 | JSON Lines, gzip 压缩 (`.json.gz`) |
| 存储位置 | `s3://adap-prototype-812046859005/github-events/` |
| 文件数 | 770 个 (含重复的 .gz.1 文件) |
| 压缩大小 | 5.4 GB |
| 总行数 | **14,323,570** |
| 时间跨度 | 2015-01-01 (16天数据) |

### 数据结构

```json
{
  "id": "34829531358",
  "type": "IssueCommentEvent",
  "actor": {"id": 733326, "login": "sylvestre", "url": "...", "avatar_url": "..."},
  "repo": {"id": 11847500, "name": "uutils/coreutils", "url": "..."},
  "payload": {"action": "created", "issue": {...深嵌套...}, "comment": {...深嵌套...}},
  "public": true,
  "created_at": "2024-01-15T12:00:00Z",
  "org": {"id": 5148717, "login": "uutils", ...}
}
```

### Payload 异构性分析

`payload` 字段是最适合做 Variant shredding 性能对比的字段：
- 结构随 `type` 完全不同（20+ 种 event type）
- 包含深嵌套对象（issue.user, pull_request.head.repo 等）
- 包含数组（commits, labels, assignees）
- 字段丰富（单个 payload 可达 50+ 字段）

---

## 表设计

### 设计原则

两张表 **仅 payload 字段不同**，其他字段完全一致（STRING），确保查询对比的公平性。

### S3 Tables 配置

- **Table Bucket**: `iceberg-data-812046859005`
- **Namespace**: `github`
- **Catalog**: `s3t` (S3TablesCatalog)

### events_json (Iceberg v2)

```sql
CREATE TABLE s3t.github.events_json (
    id          STRING,
    type        STRING,
    public      BOOLEAN,
    created_at  TIMESTAMP,
    actor       STRING,     -- JSON string
    repo        STRING,     -- JSON string
    payload     STRING,     -- JSON string ← 唯一差异
    org         STRING      -- JSON string
)
PARTITIONED BY (days(created_at), type)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
```

### events_variant (Iceberg v3 + Variant Shredding)

```sql
CREATE TABLE s3t.github.events_variant (
    id          STRING,
    type        STRING,
    public      BOOLEAN,
    created_at  TIMESTAMP,
    actor       STRING,     -- JSON string (与 json 表相同)
    repo        STRING,     -- JSON string (与 json 表相同)
    payload     VARIANT,    -- VARIANT shredding ← 唯一差异
    org         STRING      -- JSON string (与 json 表相同)
)
PARTITIONED BY (days(created_at), type)
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.shred-variants' = 'true'
)
```

### 存储对比结果

| 指标 | events_json | events_variant | 差异 |
|------|-------------|----------------|------|
| 行数 | 14,323,570 | 14,323,570 | 相同 |
| 文件数 | 449 | 449 | 相同 |
| 存储大小 | 4.44 GB | 4.92 GB | **+10.7%** |

> 注：Variant shredding 因 payload 高度异构（20+ 种结构）产生大量稀疏列，存储反而略大。
> 这验证了 shredding 的价值在于 **查询性能** 而非存储节省。

---

## 代码位置

```
s3tables-iceberg-v3-variant/
├── etl/java/src/main/java/com/example/
│   ├── CreateGithubEventsTable.java   # 建表 (两张表)
│   └── GithubEventsEtl.java          # ETL (--mode json|variant)
├── k8s/
│   ├── create-github-events-table.yaml    # 建表 SparkApplication
│   ├── github-events-etl-json.yaml        # ETL → events_json
│   └── github-events-etl-variant.yaml     # ETL → events_variant
├── docker/
│   └── Dockerfile                     # Spark 4.1.2 + Iceberg 1.11 镜像
└── docs/
    └── github-events-benchmark.md     # 本文档
```

---

## 运行环境

### EKS 集群

| 配置项 | 值 |
|--------|-----|
| 集群名称 | `emr-on-eks-cluster` |
| 命名空间 | `emr-eks-spark` |
| NodeGroup | `spark-benchmark-x86-4xl` |
| 实例类型 | `m8i.4xlarge` (16 vCPU, 64 GiB) |
| 最大节点数 | 60 |
| Node Selector | `workload-type=spark-benchmark`, `kubernetes.io/arch=amd64` |

### Docker 镜像

| 配置项 | 值 |
|--------|-----|
| 镜像 | `812046859005.dkr.ecr.us-east-1.amazonaws.com/oss-spark:4.1.2-iceberg1.11-s3t` |
| 基础镜像 | Amazon Corretto 17 (AL2023) |
| Spark | 4.1.2 (bin-hadoop3) |
| Iceberg | 1.11.0 (spark-runtime-4.1 + iceberg-aws) |
| Hadoop AWS | 3.4.1 |
| AWS SDK | 2.29.52 |
| S3 Tables Catalog | 通过 `spark.jars.packages` 引入 `software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8` |

### Spark 资源配置 (ETL / Query)

| 角色 | Cores | Memory | MemoryOverhead | Instances |
|------|-------|--------|----------------|-----------|
| Driver | 4 | 12g | 4096m | 1 |
| Executor | 8 | 28g | 4096m | 15 |

### Spark 关键参数

```yaml
spark.sql.catalog.s3t: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.s3t.catalog-impl: software.amazon.s3tables.iceberg.S3TablesCatalog
spark.sql.catalog.s3t.warehouse: arn:aws:s3tables:us-east-1:812046859005:bucket/iceberg-data-812046859005
spark.sql.parquet.variantShreddingEnabled: "true"
spark.jars.packages: "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8"
```

---

## 操作步骤

### 1. 建表

```bash
kubectl apply -f k8s/create-github-events-table.yaml
```

### 2. 上传数据

```bash
# 从 GH Archive 下载并上传到 S3
for hour in $(seq -w 0 23); do
  wget -q https://data.gharchive.org/2015-01-01-${hour}.json.gz
done
aws s3 cp . s3://adap-prototype-812046859005/github-events/ --recursive --include "*.json.gz"
```

### 3. 运行 ETL

```bash
# JSON 表
kubectl apply -f k8s/github-events-etl-json.yaml

# Variant 表
kubectl apply -f k8s/github-events-etl-variant.yaml
```

### 4. 运行查询对比

使用下面设计的 Query Test Cases 对两张表分别执行查询，对比性能。

---

## 查询测试 Cases

> 所有 Case 聚焦于 **payload 字段** 的查询差异。
> actor/repo/org 在两张表中均为 STRING，查询方式相同，不作为对比点。

---

### Case 1: payload 单字段谓词下推 (Predicate Pushdown)

**目标**: 验证 shredded 列是否能直接做谓词下推，避免全量 payload 解析。

```sql
-- JSON 表
SELECT id, type, created_at
FROM s3t.github.events_json
WHERE type = 'IssueCommentEvent'
  AND get_json_object(payload, '$.action') = 'created';

-- Variant 表
SELECT id, type, created_at
FROM s3t.github.events_variant
WHERE type = 'IssueCommentEvent'
  AND payload.action = 'created';
```

**预期**: Variant 表利用 `payload.action` 的 shredded typed_value 列做 Parquet 级别的谓词下推，跳过不匹配的 row group；JSON 表需要读取整个 payload string 再解析。

---

### Case 2: payload 深嵌套字段过滤 (Deep Nested Filter)

**目标**: 测试 shredding 对深层嵌套字段的过滤加速效果。

```sql
-- JSON 表
SELECT id, created_at,
       get_json_object(payload, '$.issue.title') AS issue_title
FROM s3t.github.events_json
WHERE type = 'IssuesEvent'
  AND CAST(get_json_object(payload, '$.issue.comments') AS INT) > 50;

-- Variant 表
SELECT id, created_at,
       payload.issue.title AS issue_title
FROM s3t.github.events_variant
WHERE type = 'IssuesEvent'
  AND payload.issue.comments > 50;
```

**预期**: Variant 表直接在 shredded 列 `payload.issue.comments` 上做数值比较；JSON 表需要解析整个 payload → 提取 issue → 提取 comments → 转 INT。

---

### Case 3: payload 聚合统计 (Aggregation)

**目标**: 测试对 payload 内字段做聚合时的性能差异。

```sql
-- JSON 表
SELECT get_json_object(payload, '$.action') AS action,
       COUNT(*) AS cnt
FROM s3t.github.events_json
WHERE type = 'WatchEvent'
   OR type = 'ForkEvent'
   OR type = 'IssuesEvent'
GROUP BY get_json_object(payload, '$.action')
ORDER BY cnt DESC;

-- Variant 表
SELECT payload.action AS action,
       COUNT(*) AS cnt
FROM s3t.github.events_variant
WHERE type = 'WatchEvent'
   OR type = 'ForkEvent'
   OR type = 'IssuesEvent'
GROUP BY payload.action
ORDER BY cnt DESC;
```

**预期**: Variant 表只读取 `payload.action` 列（列裁剪），IO 远小于读取整个 payload STRING。

---

### Case 4: payload 多字段投影 (Column Pruning)

**目标**: 验证 shredding 的列裁剪优势 — 从巨大的 payload 中只读取少量字段。

```sql
-- JSON 表
SELECT id,
       get_json_object(payload, '$.action') AS action,
       get_json_object(payload, '$.number') AS pr_number,
       get_json_object(payload, '$.pull_request.title') AS pr_title,
       get_json_object(payload, '$.pull_request.state') AS pr_state,
       get_json_object(payload, '$.pull_request.merged') AS pr_merged
FROM s3t.github.events_json
WHERE type = 'PullRequestEvent';

-- Variant 表
SELECT id,
       payload.action AS action,
       payload.number AS pr_number,
       payload.pull_request.title AS pr_title,
       payload.pull_request.state AS pr_state,
       payload.pull_request.merged AS pr_merged
FROM s3t.github.events_variant
WHERE type = 'PullRequestEvent';
```

**预期**: PullRequestEvent 的 payload 非常大（完整的 PR 对象 50+ 字段），JSON 表需要读取并解析全部内容；Variant 表只读取 5 个 shredded 列。这是 **最能体现 shredding IO 优势** 的场景。

---

### Case 5: payload 数值范围过滤 + 聚合

**目标**: 测试数值类型字段的过滤和聚合性能。

```sql
-- JSON 表
SELECT get_json_object(repo, '$.name') AS repo_name,
       COUNT(*) AS push_count,
       SUM(CAST(get_json_object(payload, '$.size') AS INT)) AS total_commits
FROM s3t.github.events_json
WHERE type = 'PushEvent'
  AND CAST(get_json_object(payload, '$.size') AS INT) > 10
GROUP BY get_json_object(repo, '$.name')
ORDER BY total_commits DESC
LIMIT 20;

-- Variant 表
SELECT get_json_object(repo, '$.name') AS repo_name,
       COUNT(*) AS push_count,
       SUM(CAST(payload.size AS INT)) AS total_commits
FROM s3t.github.events_variant
WHERE type = 'PushEvent'
  AND payload.size > 10
GROUP BY get_json_object(repo, '$.name')
ORDER BY total_commits DESC
LIMIT 20;
```

**预期**: Variant 表的 `payload.size` 作为 shredded 数值列，过滤和 SUM 直接在列上操作，无需 string→int 转换；JSON 表每行都要解析 payload 并做类型转换。

---

### Case 6: payload 数组展开 (Array Explode)

**目标**: 对比从 payload 中展开嵌套数组的性能。

```sql
-- JSON 表
SELECT id, created_at,
       commit_item.sha,
       commit_item.author.name AS author_name,
       commit_item.message
FROM s3t.github.events_json
LATERAL VIEW explode(
    from_json(
        get_json_object(payload, '$.commits'),
        'array<struct<sha:string,author:struct<name:string,email:string>,message:string,distinct:boolean>>'
    )
) t AS commit_item
WHERE type = 'PushEvent'
  AND created_at >= timestamp '2015-01-01 12:00:00'
  AND created_at < timestamp '2015-01-01 13:00:00';

-- Variant 表
SELECT id, created_at,
       explode(variant_explode_outer(payload.commits)) AS commit_item
FROM s3t.github.events_variant
WHERE type = 'PushEvent'
  AND created_at >= timestamp '2015-01-01 12:00:00'
  AND created_at < timestamp '2015-01-01 13:00:00';
```

**预期**:
- JSON 表：需要手动指定完整 schema 做 `from_json` 反序列化，schema 错误会导致数据丢失
- Variant 表：直接展开 variant 数组，无需预定义 schema，更灵活且性能更好

---

### Case 7: 全表扫描基准 (Baseline — 不涉及 payload)

**目标**: 确认不涉及 payload 字段时，两张表性能一致（排除干扰因素）。

```sql
-- JSON 表
SELECT type, COUNT(*) AS cnt,
       COUNT(DISTINCT get_json_object(actor, '$.login')) AS unique_actors
FROM s3t.github.events_json
GROUP BY type
ORDER BY cnt DESC;

-- Variant 表
SELECT type, COUNT(*) AS cnt,
       COUNT(DISTINCT get_json_object(actor, '$.login')) AS unique_actors
FROM s3t.github.events_variant
GROUP BY type
ORDER BY cnt DESC;
```

**预期**: 两者性能接近（actor 都是 STRING，type 是普通列），验证 payload 为 VARIANT 不会拖慢非 payload 相关的查询。

---

## 评测指标

| 指标 | 说明 | 获取方式 |
|------|------|----------|
| **执行时间** | 每个 query 跑 3 次取中位数 | Spark History Server |
| **Scan 数据量** | bytes read from storage | Spark UI → SQL → Details |
| **Task 数量** | 总 task 和 skipped task 数 | Spark UI → Stages |
| **GC 时间** | 垃圾回收耗时 | Spark UI → Executors |

### 测试规范

1. 每个 Case 先跑一次 warmup（不计入结果）
2. 正式跑 3 次，取中位数
3. 两张表使用相同的 executor 配置
4. 记录 `EXPLAIN FORMATTED` 的物理计划差异

---

## 预期结论

| 场景 | JSON (STRING) | Variant (Shredding) | 加速比 (预估) |
|------|---------------|---------------------|--------------|
| Case 1: 单字段过滤 | 解析整个 payload | 直接读 shredded 列 | 2-5× |
| Case 2: 深嵌套过滤 | 多层 JSON 解析 | 直接列过滤 | 3-8× |
| Case 3: 字段聚合 | 读全量 payload | 只读 action 列 | 3-10× |
| Case 4: 多字段投影 | 解析大 PR payload | 只读 5 列 | **5-20×** |
| Case 5: 数值过滤+聚合 | string→int 转换 | 原生数值操作 | 3-8× |
| Case 6: 数组展开 | from_json + explode | variant 直接展开 | 2-5× |
| Case 7: 基准 (无 payload) | — | — | ≈1× |

### 关键对比维度

1. **IO 减少**：Variant shredding 最大的优势是列裁剪 — PullRequestEvent 的 payload 平均 5-10 KB，如果只需要 5 个字段，Variant 只读 ~200 bytes，IO 减少 95%+
2. **CPU 减少**：JSON 表每行需要完整的 JSON parse + path extraction；Variant 表直接读取原生类型列
3. **谓词下推**：Variant shredded 列支持 Parquet statistics 过滤（min/max），JSON string 列无法做到
4. **语法简洁**：`payload.action` vs `get_json_object(payload, '$.action')` — 代码可读性和维护性显著提升

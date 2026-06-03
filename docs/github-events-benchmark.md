# GitHub Events — Variant Shredding vs JSON Benchmark

## 测试目标

对比 Iceberg v3 VARIANT (shredding) 与传统 JSON STRING 在以下维度的差异：

1. **存储空间** — 相同数据量下两种格式占用的存储大小
2. **查询性能** — 典型分析查询的执行时间对比
3. **写入性能** — ETL 导入相同数据的耗时对比

---

## 数据集

| 项目 | 说明 |
|------|------|
| 来源 | [GH Archive](https://www.gharchive.org/) — GitHub 公开事件归档 |
| 格式 | JSON Lines, gzip 压缩 (`.json.gz`) |
| 存储位置 | `s3://adap-prototype-812046859005/github-events/` |
| 时间范围 | TBD（上传后确认） |
| 预估数据量 | 每小时约 15-20万事件，每天约 400-500万事件 |

### 数据结构

```json
{
  "id": "34829531358",
  "type": "IssueCommentEvent",
  "actor": {"id": 733326, "login": "sylvestre", ...},
  "repo": {"id": 11847500, "name": "uutils/coreutils", ...},
  "payload": {"action": "created", "issue": {...}, "comment": {...}},
  "public": true,
  "created_at": "2024-01-15T12:00:00Z",
  "org": {"id": 5148717, "login": "uutils", ...}
}
```

### Object 字段分析

| 字段 | 结构特点 | Shredding 价值 |
|------|----------|---------------|
| `actor` | 固定 6 个字段，全部事件都有 | 中 — 字段固定但值不同 |
| `repo` | 固定 3 个字段 | 中 |
| `payload` | **高度异构**，随 type 变化 | **高** — 最能体现 shredding 优势 |
| `org` | 固定 5 个字段，可选 | 低 |

---

## 表设计

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
    payload     STRING,     -- JSON string
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
    actor       VARIANT,    -- variant shredding
    repo        VARIANT,    -- variant shredding
    payload     VARIANT,    -- variant shredding
    org         VARIANT     -- variant shredding
)
PARTITIONED BY (days(created_at), type)
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.shred-variants' = 'true'
)
```

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

### Spark 资源配置 (ETL)

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
spark.sql.adaptive.enabled: "true"
spark.sql.shuffle.partitions: "400"
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
  wget -q https://data.gharchive.org/2024-01-15-${hour}.json.gz
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

使用下面设计的 Query Test Cases 在 Spark 上对两张表分别执行查询。

---

## 查询测试 Cases

### Case 1: 单字段精确过滤 (Point Lookup)

**场景**: 按 actor.login 查找特定用户的事件

```sql
-- JSON 表
SELECT id, type, created_at
FROM s3t.github.events_json
WHERE get_json_object(actor, '$.login') = 'torvalds';

-- Variant 表
SELECT id, type, created_at
FROM s3t.github.events_variant
WHERE actor.login = 'torvalds';
```

**预期**: Variant 表利用 shredded 列做谓词下推，避免全量解析 actor JSON。

---

### Case 2: 嵌套字段聚合 (Aggregation on Nested Field)

**场景**: 统计每个仓库的事件数 Top 20

```sql
-- JSON 表
SELECT get_json_object(repo, '$.name') AS repo_name, COUNT(*) AS cnt
FROM s3t.github.events_json
GROUP BY get_json_object(repo, '$.name')
ORDER BY cnt DESC
LIMIT 20;

-- Variant 表
SELECT repo.name AS repo_name, COUNT(*) AS cnt
FROM s3t.github.events_variant
GROUP BY repo.name
ORDER BY cnt DESC
LIMIT 20;
```

**预期**: Variant 表只需读取 repo.name 的 shredded 列，IO 大幅减少。

---

### Case 3: Payload 深层嵌套字段查询

**场景**: 查找特定 issue 的评论事件

```sql
-- JSON 表
SELECT id, created_at,
       get_json_object(payload, '$.comment.body') AS comment_body
FROM s3t.github.events_json
WHERE type = 'IssueCommentEvent'
  AND get_json_object(payload, '$.action') = 'created'
  AND CAST(get_json_object(payload, '$.issue.number') AS INT) = 5698;

-- Variant 表
SELECT id, created_at,
       payload.comment.body AS comment_body
FROM s3t.github.events_variant
WHERE type = 'IssueCommentEvent'
  AND payload.action = 'created'
  AND payload.issue.number = 5698;
```

**预期**: Variant shredding 对深嵌套的 payload 有列裁剪优势，不需要解析整个 payload。

---

### Case 4: 多字段组合过滤 + 投影

**场景**: 查找特定组织中 PushEvent 的提交信息

```sql
-- JSON 表
SELECT get_json_object(actor, '$.login') AS actor_login,
       get_json_object(repo, '$.name') AS repo_name,
       get_json_object(payload, '$.size') AS push_size
FROM s3t.github.events_json
WHERE type = 'PushEvent'
  AND get_json_object(org, '$.login') = 'apache'
  AND created_at >= '2024-01-15 00:00:00'
  AND created_at < '2024-01-16 00:00:00';

-- Variant 表
SELECT actor.login AS actor_login,
       repo.name AS repo_name,
       payload.size AS push_size
FROM s3t.github.events_variant
WHERE type = 'PushEvent'
  AND org.login = 'apache'
  AND created_at >= '2024-01-15 00:00:00'
  AND created_at < '2024-01-16 00:00:00';
```

**预期**: Variant 表同时利用分区裁剪 (type + days) 和列裁剪 (shredded fields)。

---

### Case 5: 全表扫描聚合 (Worst Case for Variant)

**场景**: 统计所有事件类型的分布

```sql
-- JSON 表
SELECT type, COUNT(*) AS cnt
FROM s3t.github.events_json
GROUP BY type
ORDER BY cnt DESC;

-- Variant 表
SELECT type, COUNT(*) AS cnt
FROM s3t.github.events_variant
GROUP BY type
ORDER BY cnt DESC;
```

**预期**: 两者性能接近（不涉及 object 字段解析），验证 variant 无额外开销。

---

### Case 6: 多列提取 (Wide Projection)

**场景**: 提取 actor 全部字段

```sql
-- JSON 表
SELECT get_json_object(actor, '$.id') AS actor_id,
       get_json_object(actor, '$.login') AS actor_login,
       get_json_object(actor, '$.display_login') AS display_login,
       get_json_object(actor, '$.url') AS actor_url,
       get_json_object(actor, '$.avatar_url') AS avatar_url
FROM s3t.github.events_json
WHERE type = 'PushEvent'
LIMIT 1000;

-- Variant 表
SELECT actor.id AS actor_id,
       actor.login AS actor_login,
       actor.display_login AS display_login,
       actor.url AS actor_url,
       actor.avatar_url AS avatar_url
FROM s3t.github.events_variant
WHERE type = 'PushEvent'
LIMIT 1000;
```

**预期**: JSON 表需要解析 actor 字符串 5 次；Variant 表直接读取 5 个 shredded 列。

---

### Case 7: 复杂嵌套数组操作

**场景**: PushEvent 中提取 commits 数组的 author 信息

```sql
-- JSON 表 (需要 explode JSON array)
SELECT id, created_at, c.*
FROM s3t.github.events_json
LATERAL VIEW explode(
    from_json(
        get_json_object(payload, '$.commits'),
        'array<struct<sha:string,author:struct<name:string,email:string>,message:string>>'
    )
) t AS c
WHERE type = 'PushEvent'
  AND created_at >= '2024-01-15 00:00:00'
  AND created_at < '2024-01-15 01:00:00';

-- Variant 表
SELECT id, created_at, c.*
FROM s3t.github.events_variant,
LATERAL FLATTEN(payload.commits) AS c
WHERE type = 'PushEvent'
  AND created_at >= '2024-01-15 00:00:00'
  AND created_at < '2024-01-15 01:00:00';
```

**预期**: JSON 表需要手动指定 schema 做反序列化；Variant 表直接展开数组，语法简洁且性能更好。

---

## 评测指标

| 指标 | 说明 |
|------|------|
| **执行时间** | 每个 query 跑 3 次取平均 |
| **Scan 数据量** | Spark UI 中的 bytes read |
| **存储大小** | Iceberg snapshot summary 的 total-files-size |
| **文件数** | total-data-files |
| **写入时间** | ETL 作业的 start → finish |

---

## 预期结论

| 维度 | JSON (STRING) | Variant (Shredding) | 说明 |
|------|---------------|---------------------|------|
| 存储大小 | 基准 | ≈相当或略大 | zstd 对重复 key 压缩已很高效 |
| 点查/过滤 | 慢 (全字段解析) | **快** (列裁剪+谓词下推) | Case 1, 3, 4 |
| 聚合 | 慢 | **快** | Case 2 |
| 全表扫描 | 基准 | ≈相当 | Case 5 |
| 宽投影 | 慢 (多次解析) | **快** (直接读列) | Case 6 |
| 数组操作 | 复杂+慢 | **简洁+快** | Case 7 |
| 写入速度 | 快 (无转换开销) | 略慢 (parse_json + shred) | ETL 对比 |

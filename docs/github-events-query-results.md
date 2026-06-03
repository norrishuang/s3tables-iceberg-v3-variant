# GitHub Events 查询性能测试结果

## 测试时间

2026-06-03 03:34 UTC

## 测试环境

| 配置 | 值 |
|------|-----|
| Spark | 4.1.2 |
| Executor | 8 × (4 cores, 14g memory) |
| 数据量 | 14,323,570 行 |
| 存储 | events_json: 4.44 GB, events_variant: 4.92 GB |
| 表差异 | 仅 payload 字段不同 (STRING vs VARIANT) |
| Variant 访问函数 | `variant_get(payload, '$.path', 'type')` |
| JSON 访问函数 | `get_json_object(payload, '$.path')` |
| 测试方法 | 1 次 warmup + 3 次正式运行，取中位数 |

---

## 测试结果

| Case | 场景 | JSON (ms) | Variant (ms) | 比率 |
|------|------|-----------|--------------|------|
| Case 1 | payload 单字段谓词下推 | 3,203 | 3,728 | 0.86x |
| Case 2 | payload 深嵌套字段过滤 | 1,242 | 1,558 | 0.80x |
| Case 3 | payload 字段聚合 | 1,871 | 2,654 | 0.70x |
| Case 4 | payload 多字段投影 (PullRequest) | 4,974 | 8,802 | 0.57x |
| Case 5 | payload 数值过滤+聚合 | 2,617 | 4,387 | 0.60x |
| Case 6 | payload 多字段提取+过滤 (CreateEvent) | 1,814 | 1,759 | **1.03x** |
| Case 7 | 基准 (不涉及 payload) | 3,432 | 3,766 | 0.91x |

> **比率 > 1** 表示 Variant 更快，**< 1** 表示 JSON 更快。

---

## 结果分析

### 核心发现：当前配置下 Variant + `variant_get()` 比 JSON + `get_json_object()` 慢

所有涉及 payload 的 Case（1-5）中，Variant 表均比 JSON 表慢 14%-43%。仅 Case 6（CreateEvent 简单结构过滤）基本持平。

### 原因分析

#### 1. `variant_get()` 函数的运行时开销

`variant_get(payload, '$.path', 'type')` 在 Spark 4.1 中的实现：
- 需要在运行时解析 Variant binary 格式
- 需要按 JSON path 遍历 shredded metadata 定位目标字段
- 需要做类型转换（从 variant internal → 指定类型）

而 `get_json_object(payload, '$.path')` 虽然也解析 JSON，但 JSON string 在 Parquet 中是单列，IO 模式简单，且 `get_json_object` 经过多年优化，实现非常高效。

#### 2. Shredding 在小数据集上的 IO 优势未体现

- 数据集仅 **4.44 GB**（14M 行），所有数据都能快速读入
- 平均文件大小 **10 MB**，Parquet row group 粒度下列裁剪优势有限
- 如果数据集 100GB+，IO bound 占比增大，列裁剪才能显著减少读取量

#### 3. Variant 存储开销导致读取数据量更大

Variant 表存储 4.92 GB（比 JSON 大 10.7%），意味着即使做了列裁剪，baseline IO 也更大。

#### 4. Case 4 和 5 为何差距最大

- **Case 4 (PullRequestEvent 宽投影)**：调用了 5 次 `variant_get()`，每次都要解析 variant binary → 累积开销显著
- **Case 5 (PushEvent 数值)**：`variant_get(payload, '$.size', 'int')` 的类型转换开销在 SUM 聚合中被放大

#### 5. Case 6 为何持平

CreateEvent 的 payload 结构很简单（3-4 个字段），且查询同时做了 `type = 'CreateEvent'` 分区裁剪后数据量很小，此时两者开销都很低，差距消失。

---

## 关键结论

### 1. Variant shredding 的查询优势需要特定条件才能体现

| 条件 | 当前测试 | 需要的条件 |
|------|----------|------------|
| 数据规模 | 14M 行 / 4.4 GB | **100M+ 行 / 50+ GB** |
| 文件大小 | 10 MB | **128-256 MB** |
| 查询模式 | 全表扫描 + .collect() | 大表过滤少量行 (高选择性) |
| IO 瓶颈程度 | 低 (数据小，compute bound) | 高 (IO bound) |

### 2. `variant_get()` vs `get_json_object()` 的 CPU 开销对比

在 compute bound 场景（数据量小、全部读入内存）：
- `get_json_object` 已经高度优化，直接字符串解析效率很高
- `variant_get` 需要解析 binary variant 格式 + 元数据查找 + 类型转换，额外开销 20-40%

### 3. Variant shredding 真正的优势场景

- **超大表 (>50 GB)**：IO 成为瓶颈时，列裁剪可跳过 90%+ 的数据读取
- **高选择性过滤**：Parquet statistics (min/max) 可直接跳过整个 row group
- **极宽 payload**：payload 有 100+ 字段但只查 1-2 个字段时，IO 节省巨大
- **频繁查询同一字段**：shredded 列可被 OS page cache 缓存

---

## 后续建议

1. **增大数据集**：使用 100M+ 行（数月的 GH Archive 数据）重新测试
2. **增大文件大小**：使用 compaction 合并到 128-256 MB/文件
3. **测试高选择性查询**：WHERE 条件过滤掉 99% 数据的场景
4. **对比 Scan 数据量**：通过 Spark UI 确认 Variant 是否真的读取了更少的 bytes
5. **测试 Spark 后续版本**：`variant_get()` 实现可能持续优化

---

## 原始数据

### Case 1: Predicate Pushdown
```
JSON:    3042, 3512, 3203 → median 3203 ms
Variant: 4049, 3608, 3728 → median 3728 ms
```

### Case 2: Deep Nested Filter
```
JSON:    1242, 1550, 1136 → median 1242 ms
Variant: 1807, 1558, 1552 → median 1558 ms
```

### Case 3: Aggregation
```
JSON:    1871, 1751, 1923 → median 1871 ms
Variant: 2622, 2683, 2654 → median 2654 ms
```

### Case 4: Wide Projection
```
JSON:    4974, 5270, 4809 → median 4974 ms
Variant: 8802, 8648, 8980 → median 8802 ms
```

### Case 5: Numeric Filter + Aggregation
```
JSON:    3409, 2617, 2586 → median 2617 ms
Variant: 4387, 3940, 4571 → median 4387 ms
```

### Case 6: Multi Field Extract (CreateEvent)
```
JSON:    1814, 1788, 1948 → median 1814 ms
Variant: 1752, 1795, 1759 → median 1759 ms
```

### Case 7: Baseline (No Payload)
```
JSON:    3537, 3365, 3432 → median 3432 ms
Variant: 3771, 3756, 3766 → median 3766 ms
```

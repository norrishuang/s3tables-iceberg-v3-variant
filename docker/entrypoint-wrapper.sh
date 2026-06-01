#!/bin/bash
# Wrapper entrypoint：在启动 Spark 前清除 EMR Operator 注入的所有有害配置
#
# 问题背景：
#   这个 EKS 集群的 spark-operator 是 Amazon EMR 定制版（spark-operator-demo 7.12.0），
#   它会自动向每个 SparkApplication 的 spark.properties 注入大量 EMR 专属配置：
#   1. extraClassPath：几十个 EMR 路径（在干净容器里不存在，JVM classloader 扫描超时）
#   2. EMR 专属扩展类（EmrSparkSessionExtensions、EmrOptimizedSparkSqlParquetOutputCommitter 等）
#      这些类在开源容器里不存在，SparkSession 初始化时直接 ClassNotFoundException
#   3. EMRFS 文件系统代理（EMRFSDelegate、EMRFileSystem 等）
#
# 解决方案：
#   在启动 Spark 之前，过滤掉所有 EMR 专属配置行，只保留通用 Spark 配置。

EMRPROPS=/usr/lib/spark/conf/spark.properties

if [ -f "$EMRPROPS" ]; then
    mkdir -p /tmp/spark-conf-clean

    # 过滤掉所有 EMR 专属配置（类路径 + EMR 类名 + EMR 文件系统）
    grep -v -E \
        "extraClassPath|extraLibraryPath|extraJavaOptions|\
emr\.internal\.extensions|EmrOptimized|EmrSparkSession|\
EMRFSDelegate|EMRFileSystem|emrfs|emr/goodies|\
emr/security|emr/emrfs|hmclient|sagemaker-spark|\
s3select|hadoop-lzo|emr\.clusterId|emr\.stepId|\
emr\.releaseLabel|emr\.aws\.accountId|\
parquet\.output\.committer\.class=com\.amazon|\
fileoutputcommitter.*emr_internal|\
fileoutputcommitter\.cleanup-failures.*emr_internal|\
output\.fs\.optimized\.committer|\
fs\.s3\.buffer\.dir" \
        "$EMRPROPS" > /tmp/spark-conf-clean/spark.properties

    LINES_ORIG=$(wc -l < "$EMRPROPS")
    LINES_CLEAN=$(wc -l < /tmp/spark-conf-clean/spark.properties)
    echo "[wrapper] spark.properties: 原始 ${LINES_ORIG} 行 → 清理后 ${LINES_CLEAN} 行（删除 $((LINES_ORIG - LINES_CLEAN)) 个 EMR 配置）"

    # 为 spark-submit 创建 wrapper，把 --properties-file 参数指向干净版本
    REAL_SUBMIT=/tmp/spark-submit-real
    if [ ! -f "$REAL_SUBMIT" ]; then
        cp /opt/spark/bin/spark-submit "$REAL_SUBMIT"
        chmod +x "$REAL_SUBMIT"

        cat > /tmp/spark-submit-wrapper << 'SUBMIT_WRAPPER'
#!/bin/bash
# 把 EMR 注入的 spark.properties 替换成干净版本
NEWARGS=()
for arg in "$@"; do
    if [ "$arg" = "/usr/lib/spark/conf/spark.properties" ]; then
        NEWARGS+=("/tmp/spark-conf-clean/spark.properties")
        echo "[spark-submit-wrapper] --properties-file 已重定向到 /tmp/spark-conf-clean/spark.properties" >&2
    else
        NEWARGS+=("$arg")
    fi
done
exec /tmp/spark-submit-real "${NEWARGS[@]}"
SUBMIT_WRAPPER

        chmod +x /tmp/spark-submit-wrapper
        cp /tmp/spark-submit-wrapper /tmp/spark-submit
        chmod +x /tmp/spark-submit
        export PATH="/tmp:$PATH"
        echo "[wrapper] spark-submit wrapper 已就位（PATH 优先级）"
    fi
else
    echo "[wrapper] 未发现 EMR spark.properties，直接启动（纯净环境）"
fi

exec /opt/entrypoint-orig.sh "$@"

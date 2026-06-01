#!/bin/bash
# spark-submit wrapper：把 EMR 注入的 spark.properties 路径替换成干净版本
# /usr/lib/spark/conf/spark.properties 里包含几十个不存在的 EMR 路径 extraClassPath，
# 会导致 JVM classloader 扫描超时，SparkSession 初始化耗时 30+ 分钟。
# entrypoint-wrapper.sh 负责把干净版本写到 /tmp/spark-conf-clean/spark.properties，
# 本 wrapper 负责把 --properties-file 参数重定向到那个干净版本。

NEWARGS=()
for arg in "$@"; do
    if [ "$arg" = "/usr/lib/spark/conf/spark.properties" ]; then
        NEWARGS+=("/tmp/spark-conf-clean/spark.properties")
        echo "[spark-submit-wrapper] --properties-file 已重定向到 /tmp/spark-conf-clean/spark.properties" >&2
    else
        NEWARGS+=("$arg")
    fi
done

exec /opt/spark/bin/spark-submit-real "${NEWARGS[@]}"

# Spark 4.1.2 精简镜像 —— 使用 bin-hadoop3 预打包发行版
# 不单独下载 Hadoop，classpath 只有约 80 个 jar（vs hadoop-free + 平铺的 300+ 个）
# hadoop-aws / iceberg / aws-sdk 全部通过 --packages 运行时下载
# 参考 EMR on EKS 官方 --packages 模式

FROM amazoncorretto:17-al2023 AS builder

RUN dnf install -y tar gzip && dnf clean all

ARG SPARK_VERSION=4.1.2

# bin-hadoop3：已内置 Hadoop shaded jar，/jars/ 约 80 个文件
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/
RUN tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /tmp/ && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ──────────────────────────────────────────────
FROM amazoncorretto:17-al2023

WORKDIR /opt/spark

COPY --from=builder /opt/spark /opt/spark

# entrypoint / spark-submit wrapper（清理 EMR MutatingWebhook 注入的配置）
COPY --from=builder /opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint-orig.sh
COPY entrypoint-wrapper.sh /opt/entrypoint.sh
COPY spark-submit-wrapper.sh /opt/spark/bin/spark-submit-wrapper.sh

RUN chmod +x /opt/entrypoint-orig.sh /opt/entrypoint.sh && \
    chmod 755 /opt/entrypoint.sh /opt/entrypoint-orig.sh && \
    mv /opt/spark/bin/spark-submit /opt/spark/bin/spark-submit-real && \
    cp /opt/spark/bin/spark-submit-wrapper.sh /opt/spark/bin/spark-submit && \
    chmod 755 /opt/spark/bin/spark-submit /opt/spark/bin/spark-submit-real

RUN dnf update -y && \
    dnf install -y bash procps tar gzip shadow-utils wget && \
    dnf install -y python3.11 && \
    wget -q -O /usr/bin/tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-amd64 && \
    chmod +x /usr/bin/tini && \
    dnf remove -y wget && \
    dnf clean all && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && ln -sv /bin/bash /bin/sh && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV LD_LIBRARY_PATH=/lib64

# 修复权限
RUN chmod -R a+r /opt/spark/jars/

RUN groupadd -g 1000 hadoop && \
    useradd -u 1000 -g hadoop -m -s /bin/bash hadoop

USER hadoop:hadoop
ENV SPARK_USER=hadoop

WORKDIR /opt/spark/work-dir
ENTRYPOINT ["/opt/entrypoint.sh"]

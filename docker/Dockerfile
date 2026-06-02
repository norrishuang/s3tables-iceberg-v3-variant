# Spark 4.1.2 精简镜像 —— 使用 bin-hadoop3 预打包发行版
# hadoop-aws / iceberg / aws-sdk 预装到 /opt/spark/jars/（解决 s3a:// 鸡蛋问题）

FROM amazoncorretto:17-al2023 AS builder

RUN dnf install -y tar gzip && dnf clean all

ARG SPARK_VERSION=4.1.2
ARG HADOOP_VERSION=3.4.1
ARG ICEBERG_VERSION=1.11.0
ARG AWS_SDK_VERSION=2.29.52

# bin-hadoop3：已内置 Hadoop shaded jar
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/
RUN tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /tmp/ && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 下载必需的额外 JAR 到 /opt/spark/jars/
# hadoop-aws: S3AFileSystem 实现
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
# AWS SDK bundle: hadoop-aws 的依赖
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/
# hadoop-aws 需要的 aws-java-sdk-bundle (v1 兼容层)
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar /opt/spark/jars/
# Iceberg Spark runtime (4.1 for Spark 4.1.x — supports Variant type correctly)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/
# Iceberg AWS integration (S3 Tables REST Catalog + SigV4)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar /opt/spark/jars/

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

# NOTE: 当前固定 amd64 架构。如果 EKS 节点是 Graviton (ARM)，需改为 tini-arm64
ARG TARGETARCH=amd64
RUN dnf update -y && \
    dnf install -y bash procps tar gzip shadow-utils wget && \
    wget -q -O /usr/bin/tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-${TARGETARCH} && \
    chmod +x /usr/bin/tini && \
    dnf remove -y wget && \
    dnf clean all && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && ln -sv /bin/bash /bin/sh

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

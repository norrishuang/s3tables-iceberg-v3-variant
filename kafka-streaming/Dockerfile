# 基于项目已验证成功的 S3 Tables 镜像，添加 Python 3.11 + Kafka + S3 Tables Catalog jar
FROM 812046859005.dkr.ecr.us-east-1.amazonaws.com/oss-spark:4.1.2-iceberg1.11-s3t

USER root

# Install Python 3.11
RUN dnf install -y python3.11 python3.11-pip && \
    dnf clean all && \
    alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    alternatives --set python3 /usr/bin/python3.11 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python && \
    python3 -m pip install --no-cache-dir pyspark==4.1.2

# 创建 EMR Operator 注入的 eventLog 目录（避免 FileNotFoundException）
RUN mkdir -p /var/log/spark/apps && chmod 777 /var/log/spark/apps

# S3 Tables Catalog for Iceberg (支持 DDL 建表)
RUN curl -fSL -o /opt/spark/jars/s3-tables-catalog-for-iceberg-runtime-0.1.8.jar \
      "https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/0.1.8/s3-tables-catalog-for-iceberg-runtime-0.1.8.jar"

# Kafka Structured Streaming 依赖
RUN curl -fSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.2.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.2/spark-sql-kafka-0-10_2.13-4.1.2.jar" && \
    curl -fSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.1.2.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.2/spark-token-provider-kafka-0-10_2.13-4.1.2.jar" && \
    curl -fSL -o /opt/spark/jars/kafka-clients-3.7.2.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.2/kafka-clients-3.7.2.jar" && \
    curl -fSL -o /opt/spark/jars/commons-pool2-2.12.0.jar \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"

USER hadoop

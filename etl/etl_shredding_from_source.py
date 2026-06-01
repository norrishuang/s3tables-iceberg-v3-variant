"""
ETL - game_action_logs → 2 张 Shredding 对比表
  1. game_action_logs_variant_shredded    — VARIANT + hours(tm)，无 bucket，开启 Shredding
  2. game_action_logs_bkt_variant_shredded — VARIANT + hours(tm) + bucket(64,aid)，开启 Shredding

源表：s3tablesbucket.gamedb.game_action_logs（detail 为 STRING，需 parse_json 转 VARIANT）
运行环境：EKS + Spark 4.1.2 + Iceberg 1.11.0，variantShreddingEnabled=true
按天分批，支持断点续跑
"""
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

CATALOG   = "s3tablesbucket"
NAMESPACE = "gamedb"
SRC_TABLE = "game_action_logs"

# 两张目标表
DST_VARIANT_ONLY   = "game_action_logs_variant_shredded"
DST_BUCKET_VARIANT = "game_action_logs_bkt_variant_shredded"


def make_spark():
    spark = SparkSession.builder \
        .appName("ETL-Shredding-From-Source") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark version: {spark.version}")
    # variantShreddingEnabled 由 SparkApplication YAML 的 sparkConf 传入，无需在此读取
    # （spark.conf.get 会触发 sessionState 懒初始化 → SQLConf validator 扫描全量 classpath，导致启动卡住数十分钟）
    return spark


def create_tables(spark):
    # 表1：纯 VARIANT，无 bucket（对标 game_action_logs_variant，但多了 Shredding）
    t1 = f"{CATALOG}.{NAMESPACE}.{DST_VARIANT_ONLY}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t1} (
            aid     BIGINT,
            event   STRING,
            uin     BIGINT,
            roomid  STRING,
            lv      STRING,
            mode    STRING,
            version STRING,
            area    STRING,
            tm      TIMESTAMP,
            log_id  BIGINT,
            detail  VARIANT
        )
        USING iceberg
        PARTITIONED BY (hours(tm))
        TBLPROPERTIES (
            'format-version'                  = '3',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    log.info(f"表1 已就绪: {t1}")

    # 表2：VARIANT + bucket(64,aid)（对标 game_action_logs_bucket_variant，但多了 Shredding）
    t2 = f"{CATALOG}.{NAMESPACE}.{DST_BUCKET_VARIANT}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t2} (
            aid     BIGINT,
            event   STRING,
            uin     BIGINT,
            roomid  STRING,
            lv      STRING,
            mode    STRING,
            version STRING,
            area    STRING,
            tm      TIMESTAMP,
            log_id  BIGINT,
            detail  VARIANT
        )
        USING iceberg
        PARTITIONED BY (hours(tm), bucket(64, aid))
        TBLPROPERTIES (
            'format-version'                  = '3',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    log.info(f"表2 已就绪: {t2}")


def get_all_dates(spark):
    src_fqn = f"{CATALOG}.{NAMESPACE}.{SRC_TABLE}"
    rows = spark.sql(f"""
        SELECT DISTINCT DATE(tm) AS dt
        FROM {src_fqn}
        ORDER BY dt
    """).collect()
    dates = [r.dt for r in rows]
    log.info(f"源表共 {len(dates)} 天: {dates[0]} ~ {dates[-1]}")
    return dates


def get_written_dates(spark, dst_table):
    dst_fqn = f"{CATALOG}.{NAMESPACE}.{dst_table}"
    try:
        rows = spark.sql(f"SELECT DISTINCT DATE(tm) AS dt FROM {dst_fqn}").collect()
        done = {r.dt for r in rows}
        log.info(f"{dst_table} 已完成 {len(done)} 天")
        return done
    except Exception:
        log.info(f"{dst_table} 为空或不存在，从头开始")
        return set()


def process_day(spark, dt: date):
    """处理单天：同时写 2 张目标表，减少源表重复扫描"""
    src_fqn = f"{CATALOG}.{NAMESPACE}.{SRC_TABLE}"
    dst1    = f"{CATALOG}.{NAMESPACE}.{DST_VARIANT_ONLY}"
    dst2    = f"{CATALOG}.{NAMESPACE}.{DST_BUCKET_VARIANT}"
    dt_str  = dt.strftime("%Y-%m-%d")

    log.info(f"--- 处理 {dt_str} ---")

    # 读源数据（STRING detail → parse_json → VARIANT）
    df = spark.sql(f"""
        SELECT aid, event, uin, roomid, lv, mode, version, area, tm, log_id, detail
        FROM {src_fqn}
        WHERE DATE(tm) = DATE('{dt_str}')
    """)

    df_variant = df.select(
        F.col("aid"),
        F.col("event"),
        F.col("uin"),
        F.col("roomid"),
        F.col("lv"),
        F.col("mode"),
        F.col("version"),
        F.col("area"),
        F.col("tm"),
        F.col("log_id"),
        F.parse_json(F.col("detail")).alias("detail")
    ).cache()  # 缓存，两张表共用

    # 写表1：无 bucket，按 hours(tm) 分区
    df_variant \
        .repartition(F.hour(F.col("tm"))) \
        .writeTo(dst1) \
        .option("write-format", "parquet") \
        .append()
    log.info(f"  表1 {dt_str} 写入完成")

    # 写表2：有 bucket(64,aid)，按 hours(tm) + aid 分区
    df_variant \
        .repartition(F.hour(F.col("tm")), (F.col("aid") % 64).cast("int")) \
        .writeTo(dst2) \
        .option("write-format", "parquet") \
        .append()
    log.info(f"  表2 {dt_str} 写入完成")

    df_variant.unpersist()
    log.info(f"--- {dt_str} 全部写入完成 ---")


def main():
    spark = make_spark()
    create_tables(spark)

    all_dates = get_all_dates(spark)

    # 两张表分别计算已完成日期（取交集，确保两张表都写完才算完成）
    done1 = get_written_dates(spark, DST_VARIANT_ONLY)
    done2 = get_written_dates(spark, DST_BUCKET_VARIANT)
    both_done = done1 & done2  # 两张表都写完的日期

    pending = [d for d in all_dates if d not in both_done]
    log.info(f"总天数: {len(all_dates)}，已完成: {len(both_done)}，待处理: {len(pending)}")

    for i, dt in enumerate(pending, 1):
        log.info(f"进度: {i}/{len(pending)}")
        process_day(spark, dt)

    # 最终统计
    for tbl in [DST_VARIANT_ONLY, DST_BUCKET_VARIANT]:
        fqn = f"{CATALOG}.{NAMESPACE}.{tbl}"
        total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}").collect()[0].cnt
        log.info(f"=== {tbl} 总行数: {total:,} ===")

    spark.stop()


if __name__ == "__main__":
    main()

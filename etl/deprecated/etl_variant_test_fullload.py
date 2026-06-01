"""
ETL: game_action_logs → variant_test_no_partition + variant_test_with_bucket
源表: srccat.gamedb.game_action_logs（iceberg-data-812046859005，~40亿行）
目标: dstcat.iceberg_v3_test.variant_test_no_partition  （hours(tm) 分区）
      dstcat.iceberg_v3_test.variant_test_with_bucket   （hours(tm) + bucket(64,aid) 分区）
字段映射: log_id→event_id, aid→aid, tm→tm, parse_json(detail)→detail
按天分批处理，每天数据 cache 后写两张表，支持断点续跑
"""
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SRC_CATALOG  = "srccat"
DST_CATALOG  = "dstcat"
SRC_TABLE    = f"{SRC_CATALOG}.gamedb.game_action_logs"
DST_NO_PART  = f"{DST_CATALOG}.iceberg_v3_test.variant_test_no_partition"
DST_WITH_BKT = f"{DST_CATALOG}.iceberg_v3_test.variant_test_with_bucket"


def make_spark():
    spark = SparkSession.builder \
        .appName("etl-variant-test-fullload") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark 版本: {spark.version}")
    return spark


def get_all_dates(spark):
    # 日期范围已通过 Athena 确认：2026-04-28 ~ 2026-05-29，共 32 天
    # 源表有 746 个 snapshot，Iceberg metadata scan 极慢，直接硬编码
    from datetime import date, timedelta
    start = date(2026, 4, 28)
    end   = date(2026, 5, 29)
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    log.info(f"源表共 {len(dates)} 天: {dates[0]} ~ {dates[-1]}")
    return dates


def get_written_dates(spark, tbl):
    """目标表是首次全量写入，直接返回空集，跳过 partition 元数据扫描"""
    log.info(f"{tbl} 首次全量写入，视为 0 天已完成")
    return set()


def process_day(spark, dt):
    """读取源表某天数据，cache 后写入两张目标表"""
    dt_str = dt.strftime("%Y-%m-%d")
    log.info(f"=== 开始处理 {dt_str} ===")

    # 读取源数据，把 detail(string) 转换为 VARIANT
    df = spark.sql(f"""
        SELECT
            log_id             AS event_id,
            aid,
            tm,
            parse_json(detail) AS detail
        FROM {SRC_TABLE}
        WHERE DATE(tm) = DATE('{dt_str}')
    """).cache()

    cnt = df.count()
    log.info(f"{dt_str} 共 {cnt:,} 行")

    # 写入表1：只有 hours(tm) 分区，按 hour 预分区
    df_no_part = df.repartition(F.hour(F.col("tm")))
    df_no_part.writeTo(DST_NO_PART) \
        .option("write-format", "parquet") \
        .append()
    log.info(f"{dt_str} → {DST_NO_PART} 写入完成")

    # 写入表2：hours(tm) + bucket(64, aid) 分区，按 hour + aid%64 预分区
    df_with_bkt = df.repartition(F.hour(F.col("tm")), (F.col("aid") % 64))
    df_with_bkt.writeTo(DST_WITH_BKT) \
        .option("write-format", "parquet") \
        .append()
    log.info(f"{dt_str} → {DST_WITH_BKT} 写入完成")

    df.unpersist()
    log.info(f"=== {dt_str} 完成 ===")


def main():
    spark = make_spark()

    all_dates = get_all_dates(spark)

    # 两张表都写完该天才算完成（取交集）
    done_no_part  = get_written_dates(spark, DST_NO_PART)
    done_with_bkt = get_written_dates(spark, DST_WITH_BKT)
    done_both = done_no_part & done_with_bkt

    pending = [d for d in all_dates if d not in done_both]
    log.info(f"已完成: {len(done_both)} 天，待处理: {len(pending)} 天")

    for i, dt in enumerate(pending, 1):
        log.info(f"进度: {i}/{len(pending)}")
        process_day(spark, dt)

    # 最终行数验证
    for tbl in [DST_NO_PART, DST_WITH_BKT]:
        total = spark.read.table(tbl).count()
        log.info(f"最终行数 {tbl}: {total:,}")

    spark.stop()


if __name__ == "__main__":
    main()

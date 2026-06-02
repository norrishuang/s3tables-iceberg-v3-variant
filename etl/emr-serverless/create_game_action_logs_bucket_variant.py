from pyspark.sql import SparkSession

CATALOG = "s3tablesbucket"
TABLE = f"{CATALOG}.gamedb.game_action_logs_bucket_variant"

spark = SparkSession.builder.appName("CreateGameActionLogsBucketVariant").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f">>> Spark version: {spark.version}")

try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gamedb")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"""CREATE TABLE {TABLE} (
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
    ) USING iceberg
    PARTITIONED BY (hours(tm), bucket(64, aid))
    TBLPROPERTIES (
        'format-version' = '3',
        'write.parquet.compression-codec' = 'zstd',
        'write.parquet.shred-variants' = 'true'
    )""")
    print(f">>> SUCCESS: Table {TABLE} created")
    spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)
except Exception as e:
    print(f">>> FAILED: {e}")
    import traceback
    traceback.print_exc()
    raise
finally:
    spark.stop()

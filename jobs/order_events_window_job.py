import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.enable_checkpointing(10000)

env = StreamTableEnvironment.create(s_env)

access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ.get("AWS_REGION", "us-east-1")

env.execute_sql(f"""
CREATE CATALOG iceberg WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = '{access_key}',
  's3.secret-access-key' = '{secret_key}',
  's3.path-style-access' = 'true',
  's3.region'            = '{region}'
)
""")

env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg.db")

env.execute_sql("""
CREATE TABLE IF NOT EXISTS iceberg.db.order_events_1min_summary (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  order_count   BIGINT,
  total_amount  DOUBLE
) WITH (
  'format-version' = '2'
)
""")

# Kafka ソース: event_time から ts を計算しウォーターマークを付与
env.execute_sql("""
CREATE TEMPORARY TABLE kafka_orders (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  STRING,
  ts          AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss'),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'order_events',
  'properties.bootstrap.servers'  = 'kafka:9092',
  'properties.group.id'           = 'flink-window-consumer',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'json'
)
""")

# 1分タンブリングウィンドウで集計
env.execute_sql("""
INSERT INTO iceberg.db.order_events_1min_summary
SELECT
  window_start,
  window_end,
  COUNT(*)      AS order_count,
  SUM(amount)   AS total_amount
FROM TABLE(
  TUMBLE(TABLE kafka_orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end
""")

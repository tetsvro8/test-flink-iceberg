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

# event_time の時間単位でパーティション分割
env.execute_sql("""
CREATE TABLE IF NOT EXISTS iceberg.db.order_events_partitioned (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  TIMESTAMP(3),
  event_hour  STRING
) PARTITIONED BY (event_hour)
WITH (
  'format-version' = '2'
)
""")

env.execute_sql("""
CREATE TEMPORARY TABLE kafka_orders (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'order_events',
  'properties.bootstrap.servers'  = 'kafka:9092',
  'properties.group.id'           = 'flink-partitioned-consumer',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json'
)
""")

env.execute_sql("""
INSERT INTO iceberg.db.order_events_partitioned
SELECT
  order_id,
  user_id,
  product_id,
  amount,
  TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss'),
  DATE_FORMAT(TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd-HH')
FROM kafka_orders
""")

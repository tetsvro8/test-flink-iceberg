import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.enable_checkpointing(10000)  # 10秒ごとにチェックポイント

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
CREATE TABLE IF NOT EXISTS iceberg.db.order_events (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  TIMESTAMP(3)
) WITH (
  'format-version' = '2'
)
""")

env.execute_sql("""
CREATE TEMPORARY TABLE datagen (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  TIMESTAMP(3)
) WITH (
  'connector'              = 'datagen',
  'rows-per-second'        = '5',
  'fields.order_id.kind'   = 'sequence',
  'fields.order_id.start'  = '1',
  'fields.order_id.end'    = '1000000',
  'fields.user_id.min'     = '1',
  'fields.user_id.max'     = '1000',
  'fields.product_id.min'  = '1',
  'fields.product_id.max'  = '100',
  'fields.amount.min'      = '100.0',
  'fields.amount.max'      = '50000.0'
)
""")

env.execute_sql("""
INSERT INTO iceberg.db.order_events
SELECT order_id, user_id, product_id, amount, event_time
FROM datagen
""")

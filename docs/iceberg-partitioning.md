# Iceberg パーティショニング

## 概要

`order_events_partitioned` テーブルは `event_hour` 列（`yyyy-MM-dd-HH` 形式）で時間単位のパーティション分割を行う。

## テーブル設計

```sql
CREATE TABLE iceberg.db.order_events_partitioned (
  order_id    BIGINT,
  user_id     BIGINT,
  product_id  BIGINT,
  amount      DOUBLE,
  event_time  TIMESTAMP(3),
  event_hour  STRING          -- パーティションキー: 例 '2026-06-05-15'
) PARTITIONED BY (event_hour)
WITH ('format-version' = '2')
```

`event_hour` は Kafka のメッセージ内 `event_time` から計算する:

```sql
DATE_FORMAT(TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd-HH')
```

## ジョブの起動

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run \
  -py /opt/flink/jobs/order_events_partitioned_job.py
```

## MinIO でのパーティション確認

MinIO UI (http://localhost:9001) → `warehouse/db/order_events_partitioned/data/` を確認。

```
data/
  event_hour=2026-06-05-15/
    00000-0-xxxx.parquet
    00000-0-xxxx.parquet
    ...
  event_hour=2026-06-05-16/
    00000-0-xxxx.parquet
```

## Trino でのクエリ確認

### データ件数確認

```sql
SELECT event_hour, COUNT(*) AS cnt
FROM iceberg.db.order_events_partitioned
GROUP BY event_hour
ORDER BY event_hour;
```

### パーティションプルーニングの確認

`WHERE event_hour = '...'` を使うと対象パーティションのみスキャンされる。

```sql
-- 特定の時間帯のみスキャン（パーティションプルーニング）
SELECT *
FROM iceberg.db.order_events_partitioned
WHERE event_hour = '2026-06-05-15'
LIMIT 10;
```

### Trino からパーティション一覧を取得

```sql
SELECT "$partition_id", record_count
FROM "iceberg"."db"."order_events_partitioned$partitions";
```

## Trino 接続方法

```bash
docker exec -it test-flink-iceberg-trino-1 trino
```

接続後:

```sql
USE iceberg.db;
SHOW TABLES;
```

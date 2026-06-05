# Trino クエリガイド

Trino を使って Iceberg テーブルに SQL でクエリする手順。

## 接続

| 方法 | URL / コマンド |
|---|---|
| Web UI | http://localhost:8082 |
| CLI（コンテナ内） | `docker exec -it test-flink-iceberg-trino-1 trino` |

## 前提：データを書き込んでから使う

Trino でクエリするには、まず Flink ジョブでテーブルを作成・データ書き込みを行う必要がある。

```bash
# 1. 全サービス起動
docker compose up -d

# 2. Kafka ジョブ投入（テーブル作成 + データ書き込み開始）
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_kafka_job.py

# 3. チェックポイント完了を待つ（約10秒）
# 4. Trino でクエリ
```

## クエリ例

### CLI 接続
```bash
docker exec -it test-flink-iceberg-trino-1 trino
```

### データ確認

```sql
-- テーブル一覧
SHOW TABLES IN iceberg.db;

-- 最新10件
SELECT order_id, user_id, product_id, amount, event_time
FROM iceberg.db.order_events
ORDER BY event_time DESC
LIMIT 10;

-- 総件数
SELECT COUNT(*) FROM iceberg.db.order_events;
```

### 集計クエリ

```sql
-- ユーザー別注文数・合計金額（上位10件）
SELECT
  user_id,
  COUNT(*) AS order_count,
  ROUND(SUM(amount), 0) AS total_amount
FROM iceberg.db.order_events
GROUP BY user_id
ORDER BY total_amount DESC
LIMIT 10;

-- 商品別売上
SELECT
  product_id,
  COUNT(*) AS order_count,
  ROUND(AVG(amount), 0) AS avg_amount
FROM iceberg.db.order_events
GROUP BY product_id
ORDER BY order_count DESC
LIMIT 10;

-- 時間帯別注文数
SELECT
  date_trunc('minute', event_time) AS minute,
  COUNT(*) AS order_count
FROM iceberg.db.order_events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

## カタログの永続化について

`iceberg-rest` は SQLite で永続化しているため、`docker compose down` → `docker compose up -d` 後もテーブル定義が残る。  
ただし Flink ジョブを初回起動時に必ず投入すること（カタログのネームスペース初期化のため）。

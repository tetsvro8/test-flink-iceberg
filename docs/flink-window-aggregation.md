# Flink ウィンドウ集計

## 概要

Kafka の `order_events` を1分タンブリングウィンドウで集計し、`order_events_1min_summary` テーブルに書き込む。

## テーブル設計

```sql
CREATE TABLE iceberg.db.order_events_1min_summary (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  order_count   BIGINT,
  total_amount  DOUBLE
) WITH ('format-version' = '2')
```

## 集計ロジック

Kafka ソースに `WATERMARK` を定義し、Flink SQL の TVF (Table-Valued Function) 構文でタンブリングウィンドウを適用する。

```sql
-- Kafka ソース（ウォーターマーク付き）
CREATE TEMPORARY TABLE kafka_orders (
  ...
  event_time  STRING,
  ts          AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss'),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH ( 'connector' = 'kafka', ... )

-- 1分タンブリングウィンドウ集計
INSERT INTO iceberg.db.order_events_1min_summary
SELECT
  window_start,
  window_end,
  COUNT(*)    AS order_count,
  SUM(amount) AS total_amount
FROM TABLE(
  TUMBLE(TABLE kafka_orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end
```

**ポイント:**
- `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` — 最大5秒の遅延を許容
- `scan.startup.mode = 'latest-offset'` — ジョブ起動後の新着データのみ対象
- ウィンドウは `window_end + watermark delay` を過ぎると閉じてデータがコミットされる

## ジョブの起動

事前に不要なジョブを停止して TaskManager スロットを確保すること。

```bash
# 実行中ジョブの確認
curl -s http://localhost:8081/jobs/overview | python3 -c "
import json, sys
for j in json.load(sys.stdin)['jobs']:
    print(j['jid'], j['state'], j['name'])
"

# ジョブのキャンセル（REST API）
curl -X PATCH "http://localhost:8081/jobs/<JOB_ID>?mode=cancel"

# ウィンドウジョブの起動
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run \
  -py /opt/flink/jobs/order_events_window_job.py
```

## Trino でのクエリ確認

```sql
-- ウィンドウごとの集計結果
SELECT window_start, window_end, order_count, ROUND(total_amount, 2) AS total_amount
FROM iceberg.db.order_events_1min_summary
ORDER BY window_start;
```

出力例:

```
window_start              | window_end                | order_count | total_amount
--------------------------+---------------------------+-------------+--------------
2026-06-05 15:32:00.000   | 2026-06-05 15:33:00.000   | 175         | 4295711.03
2026-06-05 15:33:00.000   | 2026-06-05 15:34:00.000   | 297         | 8048396.77
2026-06-05 15:34:00.000   | 2026-06-05 15:35:00.000   | 298         | 7193886.06
```

## 注意事項

- TaskManager のスロット数はデフォルト 2。複数ジョブを同時実行する場合は `taskmanager.numberOfTaskSlots` を増やすか、不要なジョブを停止する
- `flink cancel` コマンドはクラスパス競合により動作しない場合がある。その際は REST API (`PATCH /jobs/<id>?mode=cancel`) を使うこと

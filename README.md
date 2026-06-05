# test-flink-iceberg

PyFlink + Apache Iceberg のローカル統合環境。

ECサイト注文イベント（order_id, user_id, product_id, amount, event_time）をストリーミング処理し、Icebergテーブルに書き込む。

## アーキテクチャ

```
Phase 1（完了）:
Flink DataGen → PyFlink (Table API) → Iceberg REST Catalog → MinIO

Phase 2（完了）:
Python Producer (faker) → Kafka → PyFlink → Iceberg → MinIO
```

## 前提条件

- Docker Desktop
- Bash

## セットアップ

### 1. 認証情報の設定

`.env` ファイルをプロジェクトルートに作成し、以下の内容を設定する。

```
MINIO_ROOT_USER=your_access_key
MINIO_ROOT_PASSWORD=your_secret_key
AWS_REGION=us-east-1
```

### 2. JARのダウンロード（初回のみ）

```bash
bash download-libs.sh
```

Iceberg / Hadoop / Kafka コネクタの依存JARを `lib/` に取得する。

### 3. Dockerイメージのビルド（初回のみ）

```bash
docker compose build
```

PyFlink入りのカスタムFlinkイメージと Python Producerイメージをビルドする（数分）。

### 4. 起動

```bash
docker compose up -d
```

| サービス | URL |
|---|---|
| Flink UI | http://localhost:8081 |
| MinIO UI | http://localhost:9001 |
| Iceberg REST Catalog | http://localhost:8181 |
| Kafka (外部) | localhost:9094 |
| Trino UI | http://localhost:8082 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

## ジョブの実行

### Phase 1: DataGen → Iceberg

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_job.py
```

### Phase 2: Kafka → Iceberg

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_kafka_job.py
```

Producer コンテナは `docker compose up -d` 時に自動起動し、Kafka へのイベント送信を開始する。

## データのクエリ（Trino）

Flink ジョブ投入後、Trino CLI で SQL クエリが実行できる。

```bash
docker exec -it test-flink-iceberg-trino-1 trino
```

```sql
SELECT COUNT(*) FROM iceberg.db.order_events;
SELECT order_id, user_id, amount, event_time FROM iceberg.db.order_events LIMIT 10;
```

詳細は [docs/trino-query.md](docs/trino-query.md) を参照。

## 動作確認

1. **Flink UI** (http://localhost:8081) でジョブが `RUNNING` になっていることを確認
2. **MinIO UI** (http://localhost:9001) で `warehouse/db/order_events/data/` にParquetファイルが生成されていることを確認（10秒ごとに追加される）
3. **Trino UI** (http://localhost:8082) でクエリ履歴を確認

## 停止

```bash
docker compose down
```

データを削除する場合:

```bash
docker compose down -v
```

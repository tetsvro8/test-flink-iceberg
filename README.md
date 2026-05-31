# test-flink-iceberg

PyFlink + Apache Iceberg のローカル統合環境。

ECサイト注文イベント（order_id, user_id, product_id, amount, event_time）をストリーミング処理し、Icebergテーブルに書き込む。

## アーキテクチャ

```
Phase 1（現在）:
Flink DataGen → PyFlink (Table API) → Iceberg REST Catalog → MinIO

Phase 2（予定）:
Python Producer (faker) → Kafka → PyFlink → Iceberg → MinIO
```

## 前提条件

- Docker Desktop
- Bash

## セットアップ

### 1. 認証情報の設定

```bash
cp .env.example .env
```

`.env` を編集してMinIOの認証情報を設定する。

### 2. JARのダウンロード（初回のみ）

```bash
bash download-libs.sh
```

Iceberg / Hadoop の依存JARを `lib/` に取得する（約95MB）。

### 3. Dockerイメージのビルド（初回のみ）

```bash
docker compose build
```

PyFlink入りのカスタムFlinkイメージをビルドする（数分）。

### 4. 起動

```bash
docker compose up -d
```

| サービス | URL |
|---|---|
| Flink UI | http://localhost:8081 |
| MinIO UI | http://localhost:9001 |
| Iceberg REST Catalog | http://localhost:8181 |

## ジョブの実行

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_job.py
```

## 動作確認

1. **Flink UI** (http://localhost:8081) でジョブが `RUNNING` になっていることを確認
2. **MinIO UI** (http://localhost:9001) で `warehouse/db/order_events/data/` にParquetファイルが生成されていることを確認（10秒ごとに追加される）

## 停止

```bash
docker compose down
```

データを削除する場合:

```bash
docker compose down -v
```

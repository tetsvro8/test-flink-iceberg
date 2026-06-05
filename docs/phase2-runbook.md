# Phase 2 動作確認手順

Python Producer → Kafka → PyFlink → Iceberg → MinIO パイプラインの起動・確認手順。

## 前提

- Phase 1 のセットアップ完了済み（`.env` 作成、Docker Desktop 起動済み）
- `feature/phase2-kafka` ブランチ（または main へのマージ後）を使用

## 手順

### 1. Kafka コネクタ JAR の追加取得

```bash
bash download-libs.sh
```

`lib/flink-sql-connector-kafka-3.3.0-1.20.jar` が追加されていることを確認する。

```bash
ls lib/
# flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
# flink-sql-connector-kafka-3.3.0-1.20.jar
# iceberg-aws-bundle-1.9.0.jar
# iceberg-flink-runtime-1.20-1.9.0.jar
```

### 2. Docker イメージのビルド

Producer コンテナの Python イメージを初回のみビルドする。

```bash
docker compose build
```

### 3. 全サービスの起動

```bash
docker compose up -d
```

起動するサービス一覧:

| サービス | 役割 |
|---|---|
| minio | オブジェクトストレージ |
| minio-init | warehouse バケット作成（起動後終了） |
| iceberg-rest | Iceberg REST Catalog |
| kafka | メッセージブローカー（KRaft モード） |
| kafka-init | `order_events` トピック作成（起動後終了） |
| jobmanager | Flink Job Manager |
| taskmanager | Flink Task Manager |
| producer | Kafka へイベント送信（自動継続） |

起動状態を確認する:

```bash
docker compose ps
```

`producer` コンテナがイベントを送信していることをログで確認する:

```bash
docker compose logs producer --tail=10
# sent: {'order_id': 1, 'user_id': 423, 'product_id': 7, 'amount': 12345.67, 'event_time': '2026-06-01 12:00:00'}
# sent: {'order_id': 2, ...}
```

### 4. Kafka ジョブの投入

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_kafka_job.py
```

### 5. 動作確認

**Flink UI** (http://localhost:8081)
- ジョブが `RUNNING` 状態であることを確認

**MinIO UI** (http://localhost:9001)
- `warehouse/db/order_events/data/` に Parquet ファイルが追加されることを確認（チェックポイントごと＝10秒間隔）

## 停止

```bash
docker compose down
```

データも含めて削除する場合:

```bash
docker compose down -v
```

## トラブルシューティング

### Kafka ジョブが起動しない

`lib/` に Kafka コネクタ JAR がない場合は `bash download-libs.sh` を再実行する。

### Producer が接続エラーを出す

`kafka-init` の完了前に起動した場合、Producer は自動リトライ（最大10回）する。`docker compose logs producer` でリトライ状況を確認する。

### MinIO に Parquet が生成されない

チェックポイントが完了しないと Iceberg にコミットされない。Flink UI の Checkpoints タブで `Completed` が増加していることを確認する。

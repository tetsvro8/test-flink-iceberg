# Flink メトリクス確認手順

Prometheus + Grafana で Flink のメトリクスを確認する手順。

## サービス一覧

| サービス | URL | 説明 |
|---|---|---|
| Flink UI | http://localhost:8081 | ジョブ・タスク状態 |
| Prometheus | http://localhost:9090 | メトリクス収集 |
| Grafana | http://localhost:3000 | ダッシュボード |

## 手順

### 1. 全サービス起動

```bash
docker compose up -d
```

### 2. Flink ジョブ投入

起動完了まで約10秒待ってからジョブを投入する。

```bash
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_kafka_job.py
```

### 3. Prometheus でターゲットを確認

**http://localhost:9090/targets** を開く。

`jobmanager:9249` と `taskmanager:9249` が両方 **State: UP** になっていることを確認する。

### 4. Grafana でダッシュボードを確認

1. **http://localhost:3000** を開く（ログイン: admin / admin）
2. 左メニュー → **Dashboards**
3. **Flink** フォルダ → **Flink Metrics** を開く

チェックポイントが完了するたびに（10秒ごと）グラフが更新される。

## ダッシュボードのパネル

| パネル | 内容 |
|---|---|
| Records In Per Second | Kafka から読み込んでいるレコード数/秒 |
| Records Out Per Second | Iceberg へ書き出しているレコード数/秒 |
| Last Checkpoint Duration | 最後のチェックポイントにかかった時間（ms） |
| JVM CPU Load | jobmanager / taskmanager の CPU 使用率（%） |
| Job Uptime | ジョブの稼働時間（秒） |
| Number of Completed Checkpoints | 完了したチェックポイントの累計数 |
| Last Checkpoint Size | 最後のチェックポイントのサイズ（bytes） |

## 停止

```bash
docker compose down
```

データも含めて完全リセットする場合:

```bash
docker compose down -v
```

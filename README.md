# streaming-lakehouse

Kafka + Apache Flink + Apache Iceberg によるストリーミングレイクハウスのローカル検証環境。

ECサイトの注文イベント（order_id, user_id, product_id, amount, event_time）をリアルタイムにストリーミング処理し、Icebergテーブルへ書き込む一連のパイプラインを、Docker Compose上で再現する。

> **このプロジェクトの目的**
> Netflixの大規模ストリーミングデータ基盤（Keystone）を参考に、ストリーミングレイクハウスの中核要素（メッセージング・ストリーム処理・テーブルフォーマット・クエリ・監視）を自分の手で組み上げ、設計上の論点を検証することを目的とした学習・検証用プロジェクト。

## 設計意図

このリポジトリは「動かすこと」自体より、ストリーミング基盤を構成する各レイヤーの役割と設計判断を理解することを主眼としている。

- **なぜ Flink + Iceberg + Kafka か**
  ストリーム処理（Flink）、ストリーミングに対応したテーブルフォーマット（Iceberg）、メッセージング（Kafka）という、Netflix Keystoneをはじめとする大規模配信基盤で採用される構成を、最小構成で再現するため。バッチ処理中心のデータ基盤経験から、ストリーミング／分散処理へ理解を広げることを意図している。
- **段階的な構築**
  まず Flink DataGen → Iceberg で書き込み経路を確立し（Phase 1）、次に Kafka を挟んで実運用に近いイベント駆動の構成へ拡張した（Phase 2）。レイヤーを一度に積まず、各段階で挙動を確認しながら進めることを意識した。
- **クエリと監視まで含める**
  Trino による分析クエリ、Prometheus / Grafana によるメトリクス監視まで含め、「データを流す」だけでなく「流れているデータを観測・検証できる」状態を一通り揃えた。

## 設計上の論点と今後の発展

ローカル検証環境のため本番分散環境の課題には踏み込めていないが、各論点の所在は意識して構築している。

### 検証の過程で意識した論点

- **Exactly-once セマンティクス**：Flinkのチェックポイントと Iceberg のコミットを跨いだ厳密一度の保証。ストリーミングからテーブルフォーマットへの書き込みで重複・欠損をどう防ぐかという観点。
- **遅延データ・順序保証**：イベントタイムでのウォーターマーク設計と、遅れて到着するイベントの扱い。処理時刻ではなくイベント時刻を基準にする際の論点。

### 今後の発展方向（未着手）

- **スケールと障害耐性**：単一ノード構成から、パーティショニング・並列度・障害復旧を前提とした構成への発展。
- **GCP上への展開**：ローカルMinIOからクラウドストレージ／マネージド構成への移行。
- **Go によるコンポーネント実装**：Producer や周辺ツールを Go で書き換え、プラットフォーム側の実装力を広げる。

## アーキテクチャ

```
Phase 1（完了）:
Flink DataGen → PyFlink (Table API) → Iceberg REST Catalog → MinIO

Phase 2（完了）:
Python Producer (faker) → Kafka → PyFlink → Iceberg → MinIO → Trino（クエリ） / Prometheus・Grafana（監視）
```

## 技術スタック

| レイヤー | 採用技術 |
|---|---|
| メッセージング | Apache Kafka |
| ストリーム処理 | Apache Flink (PyFlink / Table API) |
| テーブルフォーマット | Apache Iceberg (REST Catalog) |
| オブジェクトストレージ | MinIO (S3互換) |
| クエリエンジン | Trino |
| 監視 | Prometheus / Grafana |
| 実行環境 | Docker Compose |

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
docker exec streaming-lakehouse-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_job.py
```

### Phase 2: Kafka → Iceberg

```bash
docker exec streaming-lakehouse-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/order_events_kafka_job.py
```

Producer コンテナは `docker compose up -d` 時に自動起動し、Kafka へのイベント送信を開始する。

## データのクエリ（Trino）

Flink ジョブ投入後、Trino CLI で SQL クエリが実行できる。

```bash
docker exec -it streaming-lakehouse-trino-1 trino
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

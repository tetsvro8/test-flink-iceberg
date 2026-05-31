# アーキテクチャ

## データ

ECサイト注文イベント: `order_id, user_id, product_id, amount, event_time`

## フェーズ構成

### Phase 1（完了）
```
Flink DataGen → PyFlink → Iceberg REST Catalog → MinIO
```
成功基準: MinIO UI (`warehouse/db/order_events/data/`) にParquetファイルが生成されること

### Phase 2（予定）
```
Python Producer (faker) → Kafka → PyFlink → Iceberg → MinIO
```
成功基準: Kafkaトピック経由でリアルタイムにIcebergへ書き込まれること

## コンポーネント

| コンポーネント | 役割 | バージョン |
|---|---|---|
| Apache Flink | ストリーム処理エンジン | 1.20 |
| PyFlink | Python API | 1.20.0 |
| Apache Iceberg | テーブルフォーマット | 1.9.0 |
| Iceberg REST Catalog | カタログサービス | tabulario/iceberg-rest |
| MinIO | S3互換オブジェクトストレージ | latest |
| Apache Kafka | メッセージキュー（Phase 2） | - |

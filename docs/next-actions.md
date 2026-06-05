# ネクストアクション

## 完了済みフェーズ

- Phase 2: Kafka → PyFlink → Iceberg → MinIO
- Trino クエリ
- Flink メトリクス（Prometheus + Grafana）
- Iceberg パーティショニング（event_hour 列で時間単位分割）
- Flink ウィンドウ集計（1分タンブリング → order_events_1min_summary）

## 候補

### ① Iceberg Time Travel
Trino の `FOR TIMESTAMP AS OF` / `FOR VERSION AS OF` 構文で過去スナップショットのデータを参照する。

```sql
-- スナップショット一覧
SELECT snapshot_id, committed_at, operation
FROM "iceberg"."db"."order_events$snapshots"
ORDER BY committed_at;

-- 過去時点のデータを参照
SELECT COUNT(*) FROM iceberg.db.order_events
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-05 15:00:00 UTC';
```

実装はクエリのみで完結。Iceberg の最大の特徴を手軽に体感できる。

---

### ② Iceberg Compaction (OPTIMIZE)
checkpoint ごとに小さい Parquet ファイルが蓄積する問題を解消する。
Trino の `OPTIMIZE` でファイルをまとめ、読み取りパフォーマンスを改善する。

```sql
ALTER TABLE iceberg.db.order_events EXECUTE OPTIMIZE;

-- 古いスナップショットの削除
ALTER TABLE iceberg.db.order_events
EXECUTE expire_snapshots(retention_threshold => '7d');
```

---

### ③ Flink CEP（異常検知）
Flink の Complex Event Processing で特定パターンのイベントを検出する。
例: 同一ユーザーが1分以内に高額注文（amount > 40000）を2回以上行った場合にアラートイベントを生成。

実装量は多めだが、ストリーミング処理の応用的な内容。

# Flink ジョブ管理

`flink cancel` コマンドはクラスパス競合により動作しないため、REST API を使う。

## ジョブ一覧の確認

```bash
curl -s http://localhost:8081/jobs/overview | python3 -c "
import json, sys
jobs = json.load(sys.stdin)['jobs']
for j in jobs:
    print(f\"{j['jid']}  {j['state']:<12}  {j['name']}\")
"
```

出力例:
```
b5f1b657...  RUNNING       insert-into_iceberg.db.order_events_1min_summary
c9a9fff7...  RUNNING       insert-into_iceberg.db.order_events_partitioned
8f8c4fa8...  CANCELED      insert-into_iceberg.db.order_events_partitioned
```

## ジョブのキャンセル

```bash
curl -X PATCH "http://localhost:8081/jobs/<JOB_ID>?mode=cancel"
```

例:
```bash
curl -X PATCH "http://localhost:8081/jobs/c9a9fff73c697bc1acf6ef85088f123a?mode=cancel"
```

キャンセル後に状態が `CANCELED` になったことを確認する:
```bash
curl -s http://localhost:8081/jobs/<JOB_ID> | python3 -c "import json,sys; j=json.load(sys.stdin); print(j['state'])"
```

## TaskManager スロットの確認

スロット数の上限はデフォルト 2。新しいジョブを起動する前に空きがあるか確認する。

```bash
curl -s http://localhost:8081/taskmanagers | python3 -c "
import json, sys
for tm in json.load(sys.stdin)['taskmanagers']:
    slots = tm['slotsNumber']
    free  = tm['freeSlots']
    print(f'total={slots}  free={free}  used={slots - free}')
"
```

## ジョブ起動の流れ

1. スロットに空きがあるか確認
2. 不要なジョブをキャンセル
3. 新しいジョブを起動

```bash
# 1. スロット確認
curl -s http://localhost:8081/taskmanagers | python3 -c "
import json, sys
for tm in json.load(sys.stdin)['taskmanagers']:
    print(f\"free slots: {tm['freeSlots']}/{tm['slotsNumber']}\")
"

# 2. 不要ジョブをキャンセル（必要な場合）
curl -X PATCH "http://localhost:8081/jobs/<JOB_ID>?mode=cancel"

# 3. ジョブ起動
docker exec test-flink-iceberg-jobmanager-1 \
  /opt/flink/bin/flink run -py /opt/flink/jobs/<JOB_FILE>.py
```

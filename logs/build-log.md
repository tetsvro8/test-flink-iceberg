# ビルドログ：エラーと対策の記録

PyFlink + Iceberg + Kafka ローカル環境の構築過程で発生したエラーと対策をまとめる。

---

## Phase 1：Flink DataGen → PyFlink → Iceberg → MinIO

### 目的

DataGen コネクタでダミーデータを生成し、PyFlink 経由で Iceberg テーブルに書き込み、MinIO（S3互換）に Parquet ファイルを保存する。

---

### エラー 1：docker-compose.yml の YAML 構文エラー

**状況**  
`FLINK_PROPERTIES` を YAML マップ形式で記述したところ parse エラー。

**エラー**
```
yaml: line XX: mapping values are not allowed in this context
```

**原因**  
`FLINK_PROPERTIES` の値にネストされた YAML を書くと構文違反になる。

**対策**  
ブロックスカラー（`|`）でテキストとして渡す。

```yaml
FLINK_PROPERTIES: |
  jobmanager.rpc.address: jobmanager
```

---

### エラー 2：Iceberg JAR の 404

**状況**  
`download-libs.sh` で `iceberg-flink-runtime-1.20-1.6.1.jar` をダウンロードしようとした。

**エラー**
```
curl: (22) The requested URL returned error: 404
```

**原因**  
Flink 1.20 向けの Iceberg JAR は 1.6.1 では存在しない（リリース対象外）。

**対策**  
バージョンを `1.9.0` に変更。

```bash
iceberg-flink-runtime-1.20-1.9.0.jar
iceberg-aws-bundle-1.9.0.jar
```

---

### エラー 3：flink-conf.yaml が壊れる

**状況**  
ファイルを部分編集したあと、リンターが YAML を自動整形し必要なキーが削除された。

**エラー**
```
Error: The Flink conf contains option: taskmanager.memory.process.size ...
```

**原因**  
YAML オートフォーマットによる意図しない変更。

**対策**  
`flink-conf.yaml` を一から書き直し（Write ツールで完全上書き）。

---

### エラー 4：JobManager のメモリ不足

**状況**  
`docker compose up` で JobManager が即クラッシュ。

**エラー**
```
There is insufficient memory for the Java Runtime Environment to continue.
```

**原因**  
`flink-conf.yaml` にメモリ設定が抜けていた。

**対策**  
```yaml
jobmanager:
  memory:
    process:
      size: 1600m
taskmanager:
  memory:
    process:
      size: 1728m
```

---

### エラー 5：pip が見つからない

**状況**  
`flink:1.20-scala_2.12` 公式イメージに pip が存在しない。

**エラー**
```
exec: "pip": executable file not found in $PATH
```

**原因**  
Flink 公式イメージは Python を含まない。

**対策**  
カスタム `Dockerfile` を作成して Python + PyFlink をインストール。

```dockerfile
FROM flink:1.20-scala_2.12
RUN apt-get install -y python3 python3-pip python3-dev build-essential && \
    pip3 install apache-flink==1.20.0 && \
    ln -s /usr/bin/python3 /usr/bin/python
```

---

### エラー 6：gcc が見つからない（PyFlink ビルド失敗）

**状況**  
Dockerfile 内で `pip install apache-flink` が失敗。

**エラー**
```
gcc: error: ...
error: command 'gcc' failed
```

**原因**  
`build-essential`（C コンパイラ）が未インストール。一部 PyFlink 依存が C 拡張をビルドする。

**対策**  
Dockerfile に `build-essential` を追加（上記と同じ修正で解決）。

---

### エラー 7：`python` コマンドが見つからない

**状況**  
Flink が PyFlink ジョブを起動する際にクラッシュ。

**エラー**
```
exec: "python": executable file not found in $PATH
```

**原因**  
Flink の内部起動スクリプトは `python` コマンドを使うが、Debian 系では `python3` のみ存在する。

**対策**  
Dockerfile でシンボリックリンクを作成。

```dockerfile
RUN ln -s /usr/bin/python3 /usr/bin/python
```

---

### エラー 8：Hadoop クラスが見つからない

**状況**  
PyFlink ジョブ実行時に Java の ClassNotFoundException。

**エラー**
```
java.lang.NoClassDefFoundError: org/apache/hadoop/fs/FileSystem
```

**原因**  
Iceberg が Hadoop のクラスに依存しているが、Flink 標準イメージには含まれない。

**対策**  
`download-libs.sh` に Hadoop シェード済み JAR を追加。

```bash
flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
```

---

### エラー 9：Iceberg REST Catalog の AWS リージョンエラー

**状況**  
`iceberg-rest` コンテナが起動後すぐ停止。

**エラー**
```
software.amazon.awssdk.core.exception.SdkClientException:
  Unable to load region from any of the providers in the chain
```

**原因**  
`tabulario/iceberg-rest` イメージは `AWS_REGION` 環境変数が必須だが、未設定だった。

**対策**  
`docker-compose.yml` の `iceberg-rest` サービスに `AWS_REGION` を追加。  
`docker compose restart` では反映されず、`--force-recreate` が必要。

```bash
docker compose up --force-recreate iceberg-rest
```

---

### エラー 10：ジョブが成功するが MinIO にデータが書き込まれない

**状況**  
Flink UI でジョブが `FINISHED` になるが MinIO に Parquet ファイルが存在しない。

**原因**  
Iceberg は Flink のチェックポイント完了時にのみデータをコミットする。  
チェックポイントを有効化していなかったため、コミットされずに終了した。

**対策**  
`order_events_job.py` でチェックポイントを有効化。

```python
s_env.enable_checkpointing(10000)  # 10秒ごと
```

---

### エラー 11：TaskManager からの S3 書き込み失敗

**状況**  
JobManager はエラーなしだが、実際の書き込みを担う TaskManager が失敗。

**エラー**
```
software.amazon.awssdk.services.s3.model.S3Exception:
  The request signature we calculated does not match ...
```

**原因**  
`docker-compose.yml` の `taskmanager` サービスに AWS 認証情報の環境変数を設定していなかった。

**対策**  
`taskmanager` にも `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_REGION` を追加。

---

### Phase 1 完了

MinIO UI の `warehouse/db/order_events/data/` に Parquet ファイルが生成されることを確認。

---

## Phase 2：Python Producer → Kafka → PyFlink → Iceberg → MinIO

### 目的

faker ライブラリで注文イベントを生成し、Kafka 経由でリアルタイムに Iceberg へ書き込む。

---

### エラー 1：bitnami/kafka イメージが存在しない

**状況**  
`docker compose up -d` で Kafka イメージの pull が失敗。

**エラー**
```
Error response from daemon: failed to resolve reference "docker.io/bitnami/kafka:3.9":
docker.io/bitnami/kafka:3.9: not found
```

**調査**  
`bitnami/kafka:3.8`、`bitnami/kafka:3.7`、`bitnami/kafka:latest` もすべて not found。

**原因**  
Bitnami は Docker Hub からコンテナイメージを削除・移行済みだった。

**対策の検討**

| 候補 | 結果 |
|---|---|
| `apache/kafka:latest` | pull 成功（Kafka 4.2.0） |
| `confluentinc/cp-kafka:7.7.0` | pull 成功 |

`apache/kafka:latest`（公式）を採用。

---

### エラー 2：apache/kafka:latest (4.2.0) の KRaft ストレージエラー

**状況**  
`apache/kafka:latest`（Kafka 4.2.0）でコンテナを起動すると即クラッシュ。

**エラー**
```
Because controller.quorum.voters is not set on this controller,
you must specify one of the following:
--standalone, --initial-controllers, or --no-initial-controllers.
```

**原因**  
Kafka 4.x では KRaft のストレージフォーマット仕様が変更された。  
Docker イメージの内部初期化スクリプト（`KafkaDockerWrapper`）が新しいオプションを必要とするが、  
env var だけでは渡せない。

**対策**  
Kafka 3.x 系の公式イメージ `apache/kafka:3.9.0` を使用。  
3.9.0 は旧来の `controller.quorum.voters` 形式を使用するため、env var で完全設定が可能。

---

### bitnami と apache の env var 形式の違い

bitnami から apache に切り替えた際、env var のプレフィックスと設定項目が変わった。

| 設定項目 | bitnami 形式 | apache 形式 |
|---|---|---|
| プレフィックス | `KAFKA_CFG_*` | `KAFKA_*` |
| ノード ID | `KAFKA_CFG_NODE_ID` | `KAFKA_NODE_ID` |
| Quorum 設定 | `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS` | `KAFKA_CONTROLLER_QUORUM_VOTERS` |
| トピック作成コマンド | `kafka-topics.sh`（PATH に含まれる） | `/opt/kafka/bin/kafka-topics.sh`（フルパス必要） |
| CLUSTER_ID | 不要 | 必須（auto-set あり） |

---

### 最終構成（動作確認済み）

```yaml
kafka:
  image: apache/kafka:3.9.0
  environment:
    CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

起動ログで `Kafka Server started` および `Kafka version: 3.9.0` を確認。

---

## 教訓まとめ

| 教訓 | 詳細 |
|---|---|
| Iceberg は チェックポイント必須 | `enable_checkpointing()` を忘れると MinIO にデータが届かない |
| TaskManager も認証情報が必要 | JobManager だけでなく TaskManager にも AWS env var を設定する |
| `docker compose restart` では env var が反映されない | `--force-recreate` が必要 |
| Docker イメージはメジャーバージョンを固定する | `latest` は破壊的変更を含む場合がある |
| Bitnami のイメージは廃止される場合がある | 公式イメージ（apache/kafka 等）を優先する |

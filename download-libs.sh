#!/bin/bash
# Flink 1.20 + Iceberg 1.6.1 に必要なJARをダウンロード
set -e

ICEBERG_VERSION=1.9.0
FLINK_VERSION=1.20
AWS_SDK_VERSION=2.20.18
LIB_DIR="./lib"

mkdir -p "$LIB_DIR"

BASE_MVN="https://repo1.maven.org/maven2"

download() {
  local url="$1"
  local file="$LIB_DIR/$(basename $url)"
  [ -f "$file" ] && echo "skip: $(basename $url)" && return
  echo "downloading: $(basename $url)"
  curl -fsSL "$url" -o "$file"
}

download "$BASE_MVN/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar"
download "$BASE_MVN/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
download "$BASE_MVN/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
download "$BASE_MVN/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"

echo "Done. JARs in $LIB_DIR/"

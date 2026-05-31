# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyFlink + Apache Iceberg のローカル統合環境。ECサイト注文イベント（order_id, user_id, product_id, amount, event_time）を流す。

## アーキテクチャ
- Phase 1: Flink DataGen → PyFlink → Iceberg → MinIO（Docker Compose）
- Phase 2: Python Producer (faker) → Kafka → PyFlink → Iceberg → MinIO

## 成功基準（Phase 1）
MinIOのUIでIcebergが書き込んだParquetファイルが確認できる

# Karpathy 4原則（要約）
- 実装前に前提を明示する。曖昧なら聞く
- 最小限のコードで解決する。投機的な追加は不可
- 依頼された範囲だけを変更する
- 成功基準を先に定義し、検証可能にする

# git
- 作業する場合は作業ブランチを作成すること

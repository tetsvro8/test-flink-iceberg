#!/bin/bash
set -e

cat > /etc/trino/catalog/iceberg.properties << EOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=${MINIO_ROOT_USER}
s3.aws-secret-key=${MINIO_ROOT_PASSWORD}
s3.region=${AWS_REGION}
EOF

exec /usr/lib/trino/bin/run-trino

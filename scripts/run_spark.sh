#!/usr/bin/env bash
set -euo pipefail
# Spark 3.5.3 (Hadoop 3.3.4) + Iceberg 1.7.1 + konektor S3A.
# Pengunduhan JAR dari Maven Central diperlukan pada pemakaian pertama.
PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-aws-bundle:1.7.1,org.apache.hadoop:hadoop-aws:3.3.4"
exec spark-submit --master 'local[4]' \
  --packages "${PACKAGES}" \
  --conf spark.driver.memory=3g \
  --conf spark.ui.port=4040 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=http://rest:8181 \
  --conf spark.sql.catalog.lake.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lake.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.lake.s3.path-style-access=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=labadmin \
  --conf spark.hadoop.fs.s3a.secret.key=labpassword123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /opt/lab/scripts/04_spark_pipeline.py "$@"

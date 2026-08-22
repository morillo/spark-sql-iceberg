#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

# Spark 4.x supports Java 17/21 only; Homebrew's default java is too new.
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"

# Pinned Spark distribution matching the Iceberg runtime bundled in the JAR.
export SPARK_HOME=/usr/local/spark-versions/spark-4.1.3

# Build the JAR first
echo "Building JAR (JDK 17)..."
mvn clean package -DskipTests -q

# Run SimpleSparkTest with spark-submit
echo "Running SimpleSparkTest with spark-submit ($SPARK_HOME)..."
"$SPARK_HOME/bin/spark-submit" \
  --class com.morillo.spark.SimpleSparkTest \
  --master "local[*]" \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar

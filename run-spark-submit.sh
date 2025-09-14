#!/bin/bash

# Build the JAR first
echo "Building JAR..."
mvn clean package -DskipTests -q

# Run SparkIcebergApp with spark-submit
echo "Running SparkIcebergApp with spark-submit..."
/usr/local/spark-versions/spark-3.5.2/bin/spark-submit \
  --class com.morillo.spark.SparkIcebergApp \
  --master "local[*]" \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false" \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar
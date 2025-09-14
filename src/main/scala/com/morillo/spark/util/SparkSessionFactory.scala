package com.morillo.spark.util

import com.morillo.spark.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionFactory {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def createSparkSession(config: AppConfig): SparkSession = {
    logger.info(s"Creating Spark session for environment: ${config.environment}")

    val sessionBuilder = SparkSession.builder()
      .appName(config.sparkAppName)
      .master(config.sparkMaster)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", config.warehouseLocation)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Java 17 compatibility settings
      .config("spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED " +
        "-Djdk.reflect.useDirectMethodHandle=false")
      .config("spark.executor.extraJavaOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED " +
        "-Djdk.reflect.useDirectMethodHandle=false")

    // Environment-specific configurations
    config.environment match {
      case "local" =>
        sessionBuilder
          .config("spark.sql.warehouse.dir", config.warehouseLocation)
          .config("spark.sql.catalogImplementation", "in-memory")
          .config("spark.hadoop.fs.defaultFS", "file:///")
          .config("spark.eventLog.enabled", "false")
          .config("spark.ui.enabled", "true")
          .config("spark.ui.port", "4040")
          .config("spark.hadoop.hive.exec.dynamic.partition", "true")
          .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

      case "prod" =>
        sessionBuilder
          .config("spark.eventLog.enabled", "true")
          .config("spark.eventLog.dir", "/tmp/spark-events")
          .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
          .config("spark.sql.adaptive.maxShuffleHashJoinLocalMapThreshold", "0")

      case _ =>
        logger.warn(s"Unknown environment: ${config.environment}, using default configuration")
    }

    val session = sessionBuilder.getOrCreate()

    // Set log level to reduce verbosity in local development
    if (config.environment == "local") {
      session.sparkContext.setLogLevel("WARN")
    }

    logger.info(s"Spark session created successfully with version: ${session.version}")
    session
  }

  def stopSparkSession(spark: SparkSession): Unit = {
    logger.info("Stopping Spark session")
    spark.stop()
  }
}
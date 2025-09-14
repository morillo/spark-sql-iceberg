package com.example.spark.util

import com.example.spark.config.AppConfig
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
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config(s"spark.sql.catalog.${config.catalogName}", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.${config.catalogName}.type", config.catalogType)
      .config(s"spark.sql.catalog.${config.catalogName}.warehouse", config.warehouseLocation)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Environment-specific configurations
    config.environment match {
      case "local" =>
        sessionBuilder
          .config("spark.sql.warehouse.dir", config.warehouseLocation)
          .config("spark.sql.catalogImplementation", "hive")
          .config("spark.hadoop.fs.defaultFS", "file:///")
          .config("spark.eventLog.enabled", "false")
          .config("spark.ui.enabled", "true")
          .config("spark.ui.port", "4040")

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
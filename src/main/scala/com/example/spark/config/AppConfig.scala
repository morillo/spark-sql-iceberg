package com.example.spark.config

case class AppConfig(
  sparkAppName: String,
  sparkMaster: String,
  warehouseLocation: String,
  catalogName: String,
  catalogType: String,
  environment: String
)

object AppConfig {
  def apply(): AppConfig = {
    val env = sys.props.getOrElse("env", "local")
    env match {
      case "local" => localConfig
      case "prod" => prodConfig
      case _ => localConfig
    }
  }

  def localConfig: AppConfig = AppConfig(
    sparkAppName = "Spark Iceberg Local",
    sparkMaster = "local[*]",
    warehouseLocation = "file:///tmp/iceberg-warehouse",
    catalogName = "local_catalog",
    catalogType = "hadoop",
    environment = "local"
  )

  def prodConfig: AppConfig = AppConfig(
    sparkAppName = "Spark Iceberg Production",
    sparkMaster = "yarn",
    warehouseLocation = sys.props.getOrElse("warehouse.location", "/user/spark/warehouse"),
    catalogName = "production_catalog",
    catalogType = "hadoop",
    environment = "prod"
  )
}
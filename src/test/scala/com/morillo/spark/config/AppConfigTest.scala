package com.morillo.spark.config

import org.scalatest.funsuite.AnyFunSuite

class AppConfigTest extends AnyFunSuite {

  test("local config should have correct default values") {
    val config = AppConfig.localConfig

    assert(config.sparkAppName == "Spark Iceberg Local")
    assert(config.sparkMaster == "local[*]")
    assert(config.warehouseLocation == "file:///tmp/iceberg-warehouse")
    assert(config.catalogName == "local_catalog")
    assert(config.catalogType == "hadoop")
    assert(config.environment == "local")
  }

  test("prod config should have correct default values") {
    val config = AppConfig.prodConfig

    assert(config.sparkAppName == "Spark Iceberg Production")
    assert(config.sparkMaster == "yarn")
    assert(config.catalogName == "production_catalog")
    assert(config.catalogType == "hadoop")
    assert(config.environment == "prod")
  }

  test("apply should return local config by default") {
    val config = AppConfig()
    assert(config.environment == "local")
  }

  test("apply should return prod config when env system property is set to prod") {
    System.setProperty("env", "prod")
    try {
      val config = AppConfig()
      assert(config.environment == "prod")
    } finally {
      System.clearProperty("env")
    }
  }

  test("apply should return local config for unknown environment") {
    System.setProperty("env", "unknown")
    try {
      val config = AppConfig()
      assert(config.environment == "local")
    } finally {
      System.clearProperty("env")
    }
  }
}
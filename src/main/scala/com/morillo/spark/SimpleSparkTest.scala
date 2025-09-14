package com.morillo.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SimpleSparkTest {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Simple Spark Test")

    val spark = SparkSession.builder()
      .appName("Simple Spark Test")
      .master("local[*]")
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
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")

      logger.info("Spark session created successfully")
      println(s"Spark version: ${spark.version}")
      println(s"Spark UI available at: ${spark.sparkContext.uiWebUrl.getOrElse("N/A")}")

      // Simple test
      import spark.implicits._
      val testData = Seq((1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35))
      val df = testData.toDF("id", "name", "age")

      println("=== Test DataFrame ===")
      df.show()

      println("=== Count ===")
      println(s"Row count: ${df.count()}")

      logger.info("Simple Spark test completed successfully")

    } catch {
      case e: Exception =>
        logger.error("Test failed", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
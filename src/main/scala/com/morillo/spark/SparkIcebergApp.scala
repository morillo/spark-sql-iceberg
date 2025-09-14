package com.morillo.spark

import com.morillo.spark.config.AppConfig
import com.morillo.spark.model.User
import com.morillo.spark.service.IcebergService
import com.morillo.spark.util.SparkSessionFactory
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

object SparkIcebergApp {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Iceberg Application")

    val config = AppConfig()
    logger.info(s"Using configuration: ${config.environment}")

    val spark = SparkSessionFactory.createSparkSession(config)
    val icebergService = new IcebergService(spark, config)

    try {
      runDemo(icebergService)
    } catch {
      case e: Exception =>
        logger.error("Application failed", e)
        sys.exit(1)
    } finally {
      SparkSessionFactory.stopSparkSession(spark)
    }

    logger.info("Application completed successfully")
  }

  private def runDemo(icebergService: IcebergService): Unit = {
    logger.info("Running Iceberg demo operations")

    // Create table
    icebergService.createTable()

    // Insert sample users
    val users = generateSampleUsers()
    icebergService.insertUsers(users)

    // Read all users
    println("=== All Users ===")
    icebergService.readAllUsers().show()

    // Read specific user
    println("=== User with ID 1 ===")
    icebergService.readUserById(1) match {
      case Some(user) => println(user)
      case None => println("User not found")
    }

    // Update user
    icebergService.updateUser(1, name = Some("John Updated"), isActive = Some(false))

    // Show updated data
    println("=== After Update ===")
    icebergService.readAllUsers().show()

    // Merge operation
    val newUsers = Seq(
      User(4, "Alice", "alice@example.com"),
      User(1, "John Merged", "john.merged@example.com") // This will update existing user
    )
    icebergService.mergeUsers(newUsers)

    println("=== After Merge ===")
    icebergService.readAllUsers().show()

    // Show table snapshots
    println("=== Table Snapshots ===")
    icebergService.getTableSnapshots().show()

    // Show table history
    println("=== Table History ===")
    icebergService.getTableHistory().show()

    // Time travel - read data as of first snapshot
    Try {
      val snapshots = icebergService.getTableSnapshots().collect()
      if (snapshots.nonEmpty) {
        val firstSnapshotId = snapshots.last.getLong(1) // snapshot_id is in column 1
        println(s"=== Data at First Snapshot ($firstSnapshotId) ===")
        icebergService.readUsersAtSnapshot(firstSnapshotId).show()
      } else {
        logger.warn("No snapshots available for time travel demo")
      }
    } match {
      case Success(_) => // Success case handled above
      case Failure(exception) =>
        logger.warn(s"Could not retrieve snapshot for time travel demo: ${exception.getMessage}")
    }

    // Compact table
    icebergService.compactTable()

    logger.info("Demo completed successfully")
  }

  private def generateSampleUsers(): Seq[User] = {
    val now = new Timestamp(System.currentTimeMillis())
    Seq(
      User(1, "John Doe", "john.doe@example.com", now),
      User(2, "Jane Smith", "jane.smith@example.com", now),
      User(3, "Bob Johnson", "bob.johnson@example.com", now)
    )
  }
}
package com.morillo.spark.service

import com.morillo.spark.config.AppConfig
import com.morillo.spark.model.User
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

class IcebergService(spark: SparkSession, config: AppConfig) {
  import spark.implicits._

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val catalogName = "spark_catalog"
  private val tableName = s"$catalogName.default.users"

  def createTable(): Unit = {
    logger.info(s"Creating Iceberg table: $tableName")

    val createTableSQL = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id BIGINT,
        name STRING,
        email STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        is_active BOOLEAN
      ) USING iceberg
      TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
      )
    """

    Try(spark.sql(createTableSQL)) match {
      case Success(_) => logger.info(s"Table $tableName created successfully")
      case Failure(exception) =>
        logger.error(s"Failed to create table $tableName", exception)
        throw exception
    }
  }

  def insertUsers(users: Seq[User]): Unit = {
    logger.info(s"Inserting ${users.length} users into $tableName")

    try {
      val userDF = users.toDF()
      userDF.writeTo(tableName).append()
      logger.info(s"Successfully inserted ${users.length} users")
    } catch {
      case e: Exception =>
        logger.error("Failed to insert users", e)
        throw e
    }
  }

  def readAllUsers(): Dataset[User] = {
    logger.info(s"Reading all users from $tableName")

    try {
      spark.table(tableName).as[User]
    } catch {
      case e: Exception =>
        logger.error("Failed to read users", e)
        throw e
    }
  }

  def readUserById(id: Long): Option[User] = {
    logger.info(s"Reading user with id: $id from $tableName")

    try {
      val users = spark.table(tableName)
        .filter(col("id") === id)
        .as[User]
        .collect()

      users.headOption
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read user with id: $id", e)
        throw e
    }
  }

  def updateUser(id: Long, name: Option[String] = None, email: Option[String] = None, isActive: Option[Boolean] = None): Unit = {
    logger.info(s"Updating user with id: $id")

    try {
      var updateClause = "updated_at = current_timestamp()"

      name.foreach(_ => updateClause += s", name = '$name'")
      email.foreach(_ => updateClause += s", email = '$email'")
      isActive.foreach(active => updateClause += s", is_active = $active")

      val updateSQL = s"UPDATE $tableName SET $updateClause WHERE id = $id"
      spark.sql(updateSQL)

      logger.info(s"Successfully updated user with id: $id")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to update user with id: $id", e)
        throw e
    }
  }

  def mergeUsers(users: Seq[User]): Unit = {
    logger.info(s"Merging ${users.length} users into $tableName")

    try {
      val updatesDF = users.toDF().createOrReplaceTempView("user_updates")

      val mergeSQL = s"""
        MERGE INTO $tableName t
        USING user_updates s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
          name = s.name,
          email = s.email,
          updated_at = current_timestamp(),
          is_active = s.is_active
        WHEN NOT MATCHED THEN INSERT (id, name, email, created_at, updated_at, is_active)
        VALUES (s.id, s.name, s.email, s.created_at, current_timestamp(), s.is_active)
      """

      spark.sql(mergeSQL)
      logger.info(s"Successfully merged ${users.length} users")
    } catch {
      case e: Exception =>
        logger.error("Failed to merge users", e)
        throw e
    }
  }

  def deleteUser(id: Long): Unit = {
    logger.info(s"Deleting user with id: $id")

    try {
      val deleteSQL = s"DELETE FROM $tableName WHERE id = $id"
      spark.sql(deleteSQL)
      logger.info(s"Successfully deleted user with id: $id")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to delete user with id: $id", e)
        throw e
    }
  }

  def readUsersAtTimestamp(timestamp: Timestamp): Dataset[User] = {
    logger.info(s"Reading users at timestamp: $timestamp")

    try {
      spark.sql(s"SELECT * FROM $tableName FOR SYSTEM_TIME AS OF TIMESTAMP '$timestamp'").as[User]
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read users at timestamp: $timestamp", e)
        throw e
    }
  }

  def readUsersAtSnapshot(snapshotId: Long): Dataset[User] = {
    logger.info(s"Reading users at snapshot: $snapshotId")

    try {
      spark.sql(s"SELECT * FROM $tableName FOR SYSTEM_VERSION AS OF $snapshotId").as[User]
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read users at snapshot: $snapshotId", e)
        throw e
    }
  }

  def getTableSnapshots(): DataFrame = {
    logger.info(s"Getting snapshots for table: $tableName")

    try {
      spark.sql(s"SELECT * FROM $tableName.snapshots ORDER BY committed_at DESC")
    } catch {
      case e: Exception =>
        logger.error("Failed to get table snapshots", e)
        throw e
    }
  }

  def getTableHistory(): DataFrame = {
    logger.info(s"Getting history for table: $tableName")

    try {
      spark.sql(s"SELECT * FROM $tableName.history ORDER BY made_current_at DESC")
    } catch {
      case e: Exception =>
        logger.error("Failed to get table history", e)
        throw e
    }
  }

  def compactTable(): Unit = {
    logger.info(s"Compacting table: $tableName")

    try {
      spark.sql(s"CALL $catalogName.system.rewrite_data_files(table => '$tableName')")
      logger.info(s"Successfully compacted table: $tableName")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to compact table: $tableName", e)
        throw e
    }
  }
}
package com.example.spark.service

import com.example.spark.config.AppConfig
import com.example.spark.model.User
import com.example.spark.util.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import java.sql.Timestamp

class IcebergServiceTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var icebergService: IcebergService = _
  private val testWarehouse = "/tmp/test-iceberg-warehouse"

  override def beforeAll(): Unit = {
    val config = AppConfig.localConfig.copy(
      warehouseLocation = s"file://$testWarehouse",
      catalogName = "test_catalog"
    )
    spark = SparkSessionFactory.createSparkSession(config)
    icebergService = new IcebergService(spark, config)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      SparkSessionFactory.stopSparkSession(spark)
    }
    // Clean up test warehouse
    deleteDirectory(new File(testWarehouse))
  }

  override def beforeEach(): Unit = {
    // Create a fresh table for each test
    icebergService.createTable()
  }

  override def afterEach(): Unit = {
    // Clean up after each test
    try {
      spark.sql("DROP TABLE IF EXISTS test_catalog.default.users")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }

  test("create table should create Iceberg table successfully") {
    // Table creation happens in beforeEach, so just verify it exists
    val tables = spark.sql("SHOW TABLES IN test_catalog.default").collect()
    assert(tables.exists(_.getString(1) == "users"))
  }

  test("insert and read users should work correctly") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    val result = icebergService.readAllUsers().collect()
    assert(result.length == 3)
    assert(result.map(_.name).sorted === Array("Alice", "Bob", "Charlie"))
  }

  test("read user by id should return correct user") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    val user = icebergService.readUserById(1)
    assert(user.isDefined)
    assert(user.get.name == "Alice")
    assert(user.get.email == "alice@test.com")
  }

  test("read user by non-existent id should return None") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    val user = icebergService.readUserById(999)
    assert(user.isEmpty)
  }

  test("update user should modify user data") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    icebergService.updateUser(1, name = Some("Alice Updated"), isActive = Some(false))

    val updatedUser = icebergService.readUserById(1)
    assert(updatedUser.isDefined)
    assert(updatedUser.get.name == "Alice Updated")
    assert(!updatedUser.get.is_active)
  }

  test("merge users should insert new and update existing") {
    val initialUsers = createTestUsers()
    icebergService.insertUsers(initialUsers)

    val mergeUsers = Seq(
      User(1, "Alice Merged", "alice.merged@test.com"), // Update existing
      User(4, "David", "david@test.com") // Insert new
    )

    icebergService.mergeUsers(mergeUsers)

    val allUsers = icebergService.readAllUsers().collect()
    assert(allUsers.length == 4)

    val aliceUpdated = icebergService.readUserById(1)
    assert(aliceUpdated.isDefined)
    assert(aliceUpdated.get.name == "Alice Merged")

    val david = icebergService.readUserById(4)
    assert(david.isDefined)
    assert(david.get.name == "David")
  }

  test("delete user should remove user from table") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    icebergService.deleteUser(2)

    val remainingUsers = icebergService.readAllUsers().collect()
    assert(remainingUsers.length == 2)
    assert(!remainingUsers.exists(_.id == 2))
  }

  test("table snapshots should be available") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    val snapshots = icebergService.getTableSnapshots().collect()
    assert(snapshots.nonEmpty)
  }

  test("table history should be available") {
    val users = createTestUsers()
    icebergService.insertUsers(users)

    val history = icebergService.getTableHistory().collect()
    assert(history.nonEmpty)
  }

  private def createTestUsers(): Seq[User] = {
    val now = new Timestamp(System.currentTimeMillis())
    Seq(
      User(1, "Alice", "alice@test.com", now),
      User(2, "Bob", "bob@test.com", now),
      User(3, "Charlie", "charlie@test.com", now)
    )
  }

  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      directory.listFiles().foreach { file =>
        if (file.isDirectory) {
          deleteDirectory(file)
        } else {
          file.delete()
        }
      }
      directory.delete()
    }
  }
}
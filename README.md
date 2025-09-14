# Spark SQL Iceberg Example

A comprehensive Scala Maven project demonstrating Apache Spark integration with Apache Iceberg for modern data lakehouse operations.

## Features

- **Apache Spark 3.5.2** with Scala 2.12.18
- **Apache Iceberg 1.4.3** integration
- Complete CRUD operations on Iceberg tables
- Time travel queries and snapshot management
- Environment-based configuration (local/prod)
- Comprehensive unit tests
- IntelliJ IDEA integration with run configurations
- Maven-based build system with fat JAR support

## Project Structure

```
spark-sql-iceberg/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/morillo/spark/
│   │   │       ├── config/          # Configuration management
│   │   │       ├── model/           # Data models
│   │   │       ├── service/         # Business logic
│   │   │       ├── util/            # Utilities
│   │   │       └── SparkIcebergApp.scala  # Main application
│   │   └── resources/
│   │       ├── sample-users.json    # Sample data
│   │       └── logback.xml          # Logging configuration
│   └── test/
│       └── scala/                   # Unit tests
├── .idea/
│   └── runConfigurations/           # IntelliJ run configs
├── pom.xml                          # Maven configuration
└── README.md
```

## Prerequisites

- Java 8 or higher
- Scala 2.12.x
- Maven 3.6+
- IntelliJ IDEA (recommended)

## Setup

### 1. Clone and Import

1. Clone this repository
2. Open IntelliJ IDEA
3. Select "Open" and navigate to the project directory
4. Choose "Import as Maven Project" when prompted
5. Wait for Maven to download dependencies

### 2. Configure IntelliJ

1. Go to **File → Project Structure → Modules**
2. Ensure `src/main/scala` is marked as **Sources**
3. Ensure `src/test/scala` is marked as **Test Sources**
4. Ensure `src/main/resources` is marked as **Resources**
5. Set Project SDK to Java 8+

### 3. Verify Setup

Run the tests to verify everything is working:

```bash
mvn test
```

## Running the Application

### From IntelliJ IDEA

The project includes pre-configured run configurations:

1. **SparkIcebergApp (Local)** - Runs with local Spark configuration
2. **SparkIcebergApp (Prod)** - Runs with production-like configuration
3. **Run Tests** - Executes all unit tests

Simply select the desired configuration and click Run.

### From Command Line

#### Local Development

```bash
# Compile the project
mvn clean compile

# Run with local configuration
mvn exec:java -Dexec.mainClass="com.morillo.spark.SparkIcebergApp" -Denv=local

# Run tests
mvn test
```

#### Production Build

```bash
# Create fat JAR
mvn clean package

# Run with spark-submit (requires Spark installation)
spark-submit \
  --class com.morillo.spark.SparkIcebergApp \
  --master local[*] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar
```

## Configuration

The application uses environment-based configuration:

### Local Configuration (`env=local`)
- Spark Master: `local[*]`
- Warehouse: `file:///tmp/iceberg-warehouse`
- Catalog: `local_catalog`

### Production Configuration (`env=prod`)
- Spark Master: `yarn`
- Warehouse: Configurable via system property
- Catalog: `production_catalog`

### Environment Variables

Set the environment using the `env` system property:

```bash
-Denv=local   # Default, uses local configuration
-Denv=prod    # Uses production configuration
```

## Iceberg Operations

The application demonstrates various Iceberg operations:

### Table Operations
- **Create Table**: Creates Iceberg table with proper schema
- **Insert Data**: Bulk insert operations
- **Read Data**: Query operations with filtering
- **Update Data**: In-place updates
- **Merge Data**: Upsert operations
- **Delete Data**: Row deletions

### Time Travel
- **Snapshot Queries**: Read data from specific snapshots
- **Timestamp Queries**: Read data as of specific timestamps
- **History**: View table change history
- **Compaction**: Optimize table storage

### Example Operations

```scala
// Create service
val icebergService = new IcebergService(spark, config)

// Create table
icebergService.createTable()

// Insert users
val users = Seq(User(1, "John", "john@example.com"))
icebergService.insertUsers(users)

// Read all users
val allUsers = icebergService.readAllUsers().collect()

// Time travel
val snapshot = icebergService.readUsersAtSnapshot(snapshotId)
```

## Testing

The project includes comprehensive tests:

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=IcebergServiceTest

# Run tests with verbose output
mvn test -Dtest=IcebergServiceTest -DforkCount=1 -DreuseForks=false
```

## Building JAR for Production

Create a fat JAR suitable for spark-submit:

```bash
mvn clean package

# The JAR will be created at:
# target/spark-sql-iceberg-1.0-SNAPSHOT.jar
```

## Troubleshooting

### Common Issues

1. **Java Version**: Ensure Java 8+ is being used
2. **Scala Version**: Project uses Scala 2.12.18 compatible with Spark 3.5.2
3. **Memory**: Increase JVM heap if running locally: `-Xmx4g`
4. **Warehouse Directory**: Ensure write permissions to warehouse location

### Logging

The application uses Logback for logging. Configuration is in `src/main/resources/logback.xml`.

- **Console Output**: INFO level and above
- **File Output**: Logs to `logs/spark-iceberg.log`
- **Spark Logging**: Set to WARN to reduce verbosity

### Performance Tips

1. **Local Development**: Use `local[*]` for maximum parallelism
2. **Memory**: Allocate sufficient memory for Spark driver and executors
3. **Warehouse Location**: Use local filesystem for development, distributed storage for production

## Dependencies

### Core Dependencies
- Apache Spark 3.5.2
- Apache Iceberg 1.4.3
- Scala 2.12.18
- Hadoop 3.3.4

### Testing Dependencies
- ScalaTest 3.2.17
- JUnit integration

### Build Plugins
- scala-maven-plugin 4.8.1
- maven-shade-plugin 3.4.1 (for fat JAR)
- scalatest-maven-plugin 2.2.0

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is provided as an example for educational purposes.
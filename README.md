# Spark SQL Iceberg Example

A comprehensive Scala Maven project demonstrating Apache Spark integration with Apache Iceberg for modern data lakehouse operations.

## Features

- **Apache Spark 4.1.3** with Scala 2.13.17
- **Apache Iceberg 1.11.0** integration (`iceberg-spark-runtime-4.1`)
- Complete CRUD operations on Iceberg tables
- Time travel queries and snapshot management
- Environment-based configuration (local/prod)
- Comprehensive unit tests
- IntelliJ IDEA integration with run configurations
- Maven-based build system with fat JAR support

## Version Pinning (important)

Homebrew's `apache-spark` always tracks the latest Spark release, which is often
**ahead of what Iceberg supports**. As of August 2026, Homebrew ships Spark
4.2.0, but the newest published Iceberg Spark runtime targets Spark 4.1 (and the
4.1 runtime is binary-incompatible with Spark 4.2 — `IncompatibleClassChangeError`
on `connector.catalog.View`). This project therefore runs against a **pinned
Spark distribution**, the same approach used previously with Spark 3.5.2:

```
/usr/local/spark-versions/spark-4.1.3   <- used by the run scripts (SPARK_HOME)
```

Install/refresh it with:

```bash
curl -o /tmp/spark.tgz https://dlcdn.apache.org/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
tar xzf /tmp/spark.tgz -C /usr/local/spark-versions
mv /usr/local/spark-versions/spark-4.1.3-bin-hadoop3 /usr/local/spark-versions/spark-4.1.3
```

Keep `spark.version` in `pom.xml` aligned with this distribution. When Iceberg
publishes a Spark 4.2 runtime, bump both together.

## Prerequisites

- **Java 17** (Spark 4.x supports 17/21 only; Homebrew's default `java`/Maven
  JDK is too new — the build enforces this and fails fast otherwise)
- Maven 3.8+
- Pinned Spark distribution (see above) for `spark-submit`
- IntelliJ IDEA (recommended)

All Maven commands need JDK 17:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

The run scripts (`run-spark-submit.sh`, `run-simple-test.sh`) set this
automatically.

## Setup

### 1. Clone and Import

1. Clone this repository
2. Open IntelliJ IDEA
3. Select "Open" and navigate to the project directory
4. Choose "Import as Maven Project" when prompted
5. Wait for Maven to download dependencies

### 2. Configure IntelliJ

1. Set the **Project SDK to a Java 17 JDK** (e.g. Homebrew's `openjdk@17` via
   the stable path `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`,
   or the `/Library/Java/JavaVirtualMachines/openjdk-17.jdk` symlink)
2. Ensure `src/main/scala` is Sources, `src/test/scala` is Test Sources, and
   `src/main/resources` is Resources (the Maven import does this automatically)

Spark dependencies use Maven `provided` scope (they come from the Spark
distribution when using spark-submit). The bundled run configurations already
enable **"Add dependencies with 'provided' scope to classpath"**, so running
inside IntelliJ just works. If you create a new Application run configuration
manually, enable that option (Modify options → Add dependencies with "provided"
scope to classpath) and copy the `--add-opens` VM options from an existing
configuration.

### 3. Verify Setup

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn test
```

## Running the Application

### From IntelliJ IDEA

The project includes pre-configured run configurations:

1. **SparkIcebergApp (Working)** - the full Iceberg demo
2. **SimpleSparkTest (Working)** - minimal Spark smoke test
3. **Run Tests** - executes all unit tests

Simply select the desired configuration and click Run.

### From Command Line

#### One-shot scripts

```bash
./run-spark-submit.sh    # build + spark-submit the Iceberg demo
./run-simple-test.sh     # build + spark-submit the smoke test
```

#### Manually

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Build the fat JAR (app classes + Iceberg runtime; Spark stays provided)
mvn clean package -DskipTests

# Run with the pinned Spark distribution
/usr/local/spark-versions/spark-4.1.3/bin/spark-submit \
  --class com.morillo.spark.SparkIcebergApp \
  --master "local[*]" \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar
```

Catalog/extension settings are applied in code by `SparkSessionFactory`, so no
`--conf` flags are needed.

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

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Run all tests
mvn test

# Run a specific suite
mvn test -Dsuites=com.morillo.spark.service.IcebergServiceTest
```

## Troubleshooting

### Common Issues

1. **`getSubject is not supported` / enforcer failure**: Maven is running on a
   too-new JDK (Homebrew's default). `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
2. **`IncompatibleClassChangeError: ... connector.catalog.View`**: the JAR was
   submitted to Spark 4.2+ (e.g. Homebrew's `spark-submit` on PATH). Use the
   pinned distribution in `/usr/local/spark-versions/spark-4.1.3`.
3. **Kryo `HeapByteBuffer` serializer errors in the IDE**: the run
   configuration is missing the `--add-opens` VM options — copy them from a
   bundled configuration.
4. **Memory**: Increase JVM heap if running locally: `-Xmx4g`
5. **Warehouse Directory**: Ensure write permissions to warehouse location

### Logging

Logging goes through SLF4J to Spark's bundled Log4j 2 implementation. In local
mode the app lowers Spark's log level to WARN after startup.

## Dependencies

### Core Dependencies
- Apache Spark 4.1.3 (`provided` — supplied by the Spark distribution / IDE classpath)
- Apache Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13`, bundled in the fat JAR)
- Scala 2.13.17 (`provided`)

### Testing Dependencies
- ScalaTest 3.2.19
- JUnit integration

### Build Plugins
- scala-maven-plugin 4.9.2
- maven-shade-plugin 3.6.0 (for fat JAR)
- scalatest-maven-plugin 2.2.0
- maven-enforcer-plugin 3.5.0 (JDK guard)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is provided as an example for educational purposes.

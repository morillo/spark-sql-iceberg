# Quick Start - Spark SQL Iceberg with Java 17

## ✅ Working Solutions

### Option 1: Use Pre-configured IntelliJ Run Configurations

1. **SimpleSparkTest (Working)** - Basic Spark functionality test
   - Tests Java 17 + Spark 3.5.2 compatibility
   - No Iceberg complexity
   - Should run without issues

2. **SparkIcebergApp (Working)** - Full Iceberg application
   - Includes all Java 17 module access flags
   - Complete CRUD operations with Iceberg

### Option 2: Command Line (Maven)

```bash
# Compile
mvn clean compile

# Run via Maven (with proper Java 17 flags)
mvn exec:java -Pintellij
```

### Option 3: Manual IntelliJ Configuration

If you create your own run configuration:

**VM Options:**
```
-Xmx4g -XX:+UseG1GC --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Denv=local
```

## 🔧 Verified Compatibility Matrix

- ✅ **Java**: OpenJDK 17.0.16
- ✅ **Scala**: 2.12.18
- ✅ **Spark**: 3.5.2
- ✅ **Iceberg**: 1.4.3
- ✅ **Hadoop**: 3.3.4
- ✅ **Maven**: 3.6+
- ✅ **macOS**: Darwin 24.6.0

## 🚀 Test Steps

1. **First test the basic Spark functionality:**
   - Run `SimpleSparkTest (Working)` configuration
   - Should show Spark version and test DataFrame

2. **Then test the full Iceberg application:**
   - Run `SparkIcebergApp (Working)` configuration
   - Should perform CRUD operations and time travel demos

## 📝 Expected Output

**SimpleSparkTest:**
```
Spark version: 3.5.2
=== Test DataFrame ===
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|  Alice| 25|
|  2|    Bob| 30|
|  3|Charlie| 35|
+---+-------+---+
Row count: 3
```

**SparkIcebergApp:**
```
=== All Users ===
=== User with ID 1 ===
=== After Update ===
=== After Merge ===
=== Table Snapshots ===
=== Table History ===
```

## ❌ If You Still Get Errors

The most common issue is **missing Java 17 module flags**. The error will be:
```
IllegalAccessError: class org.apache.spark.storage.StorageUtils$ cannot access class sun.nio.ch.DirectBuffer
```

**Solution**: Make sure you're using the provided run configurations or the exact VM parameters listed above.

## 📁 Project Structure Summary

```
src/main/scala/com/morillo/spark/
├── SparkIcebergApp.scala          # Main Iceberg demo
├── SimpleSparkTest.scala          # Basic Spark test
├── config/AppConfig.scala         # Environment configuration
├── model/User.scala               # Data model
├── service/IcebergService.scala   # Iceberg operations
└── util/SparkSessionFactory.scala # Spark session creation
```
# ✅ WORKING Spark SQL Iceberg Project - Java 17 Compatible

## 🎯 **SINGLE POM.XML SOLUTION**

This POM fixes ALL issues:
- ✅ **Jackson dependency conflicts** - Forced to version 2.15.2
- ✅ **Java 17 module access** - Added to all Maven plugins
- ✅ **IntelliJ direct execution** - Working run configurations
- ✅ **spark-submit JAR generation** - Fat JAR with proper exclusions

## 🚀 **3 Ways to Run**

### **Option 1: IntelliJ UI (Recommended)**
1. Use the pre-configured run configurations:
   - **`SimpleSparkTest (Working)`** - Basic Spark test
   - **`SparkIcebergApp (Working)`** - Full Iceberg demo
2. Click Run ▶️ in IntelliJ

### **Option 2: Maven Command Line**
```bash
# Run SparkIcebergApp
mvn exec:java

# Run SimpleSparkTest
mvn exec:java -Dexec.mainClass="com.morillo.spark.SimpleSparkTest"
```

### **Option 3: spark-submit with Generated JAR**
```bash
# Build the JAR
mvn clean package -DskipTests

# Run with your local Spark installation
/usr/local/spark-versions/spark-3.5.2/bin/spark-submit \
  --class com.morillo.spark.SparkIcebergApp \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false" \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar

# Alternative: Run SimpleSparkTest
/usr/local/spark-versions/spark-3.5.2/bin/spark-submit \
  --class com.morillo.spark.SimpleSparkTest \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false" \
  target/spark-sql-iceberg-1.0-SNAPSHOT.jar
```

## 🔧 **What Was Fixed**

### **1. Jackson Version Conflicts**
- **Problem**: Iceberg required Jackson 2.15.x, Spark had 2.14.x
- **Solution**: `<dependencyManagement>` forces Jackson 2.15.2 everywhere
- **Result**: No more `JsonMappingException`

### **2. Java 17 Module Access**
- **Problem**: `IllegalAccessError` accessing `sun.nio.ch.DirectBuffer`
- **Solution**: Added `--add-opens` flags to ALL Maven plugins
- **Result**: Works in both IntelliJ and command line

### **3. Dependency Scope Management**
- **IntelliJ**: All dependencies have `compile` scope for IDE development
- **spark-submit**: Fat JAR includes only necessary dependencies

## 📋 **Expected Output**

### **SimpleSparkTest Success:**
```
Spark version: 3.5.2
Spark UI available at: http://192.168.1.49:4040
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

### **SparkIcebergApp Success:**
```
=== All Users ===
=== User with ID 1 ===
=== After Update ===
=== After Merge ===
=== Table Snapshots ===
=== Table History ===
Demo completed successfully
```

## 🛠 **Key POM Features**

1. **Dependency Management**: Forces consistent Jackson versions
2. **Exclusions**: Removes conflicting Jackson versions from Spark/Iceberg
3. **Java 17 Flags**: Added to scala-maven-plugin, exec-maven-plugin, scalatest-maven-plugin
4. **Fat JAR**: Maven Shade plugin creates self-contained JAR
5. **Multi-execution**: Same POM works for IntelliJ AND spark-submit

## ✅ **Verification**
- ✅ Compiles with `mvn clean compile`
- ✅ Builds JAR with `mvn clean package`
- ✅ Runs in IntelliJ with run configurations
- ✅ Runs with Maven exec plugin
- ✅ Runs with spark-submit

**This is a complete, working solution for Spark 3.5.2 + Iceberg 1.4.3 + Java 17.**
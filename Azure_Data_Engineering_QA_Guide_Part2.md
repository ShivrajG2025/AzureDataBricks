# Azure Data Engineering Q&A Guide - Part 2

## 9. Data Processing Functions

### Q9: Explode Function

**Answer:**

The `explode()` function in PySpark transforms array or map columns into multiple rows, creating one row for each element in the array/map.

**Syntax:**
```python
from pyspark.sql.functions import explode, col

# Explode array column
df_exploded = df.select(col("id"), explode(col("array_column")).alias("exploded_value"))

# Explode map column
df_exploded = df.select(col("id"), explode(col("map_column")).alias("key", "value"))
```

**Examples:**

**1. Array Explosion:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

# Sample data
data = [("John", "Python,Java,Scala"), ("Jane", "R,Python")]
df = spark.createDataFrame(data, ["name", "skills"])

# Split and explode
result = df.select("name", explode(split("skills", ",")).alias("skill"))
result.show()

# Output:
# +----+------+
# |name| skill|
# +----+------+
# |John|Python|
# |John|  Java|
# |John| Scala|
# |Jane|     R|
# |Jane|Python|
# +----+------+
```

**2. Map/Struct Explosion:**
```python
from pyspark.sql.functions import explode, map_from_arrays, array, lit

# Create map column
df_with_map = df.withColumn("skill_map", 
    map_from_arrays(
        array(lit("primary"), lit("secondary")), 
        split("skills", ",")
    ))

# Explode map
exploded_map = df_with_map.select("name", explode("skill_map").alias("skill_type", "skill"))
```

**3. Nested Structure Explosion:**
```python
# For nested arrays in structs
df.select("id", explode("nested.array_field").alias("exploded_item"))
```

**Related Functions:**
- `posexplode()`: Includes position index
- `explode_outer()`: Includes null/empty arrays
- `flatten()`: Flattens nested arrays

---

### Q10: Audit Log Table in Delta Lake Table

**Answer:**

Audit logging in Delta Lake can be implemented through multiple approaches to track data changes and access patterns.

**1. Built-in Transaction Log:**
```python
# Delta Lake automatically maintains transaction log
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/table")

# Get operation history
history = delta_table.history()
history.select("version", "timestamp", "operation", "operationParameters", "userId").show()
```

**2. Custom Audit Table Implementation:**
```python
from pyspark.sql.functions import current_timestamp, current_user, lit
from datetime import datetime

def create_audit_table():
    audit_schema = """
        table_name STRING,
        operation STRING,
        user_id STRING,
        timestamp TIMESTAMP,
        affected_rows LONG,
        old_values STRING,
        new_values STRING,
        session_id STRING
    """
    
    return spark.createDataFrame([], audit_schema)

def log_audit_event(table_name, operation, affected_rows=0, details=None):
    audit_record = [
        (table_name, operation, current_user(), datetime.now(), 
         affected_rows, details.get('old', ''), details.get('new', ''), 
         spark.sparkContext.applicationId)
    ]
    
    audit_df = spark.createDataFrame(audit_record, audit_schema)
    audit_df.write.format("delta").mode("append").save("/audit/logs")
```

**3. Change Data Feed (CDF):**
```python
# Enable CDF on table creation
spark.sql("""
    CREATE TABLE my_table (id INT, name STRING, value DOUBLE)
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Query changes
changes = spark.read.format("delta")
    .option("readChangeDataFeed", "true")
    .option("startingVersion", 0)
    .table("my_table")

changes.show()
```

**4. Comprehensive Audit Solution:**
```python
class DeltaAuditLogger:
    def __init__(self, audit_table_path):
        self.audit_path = audit_table_path
        self.init_audit_table()
    
    def init_audit_table(self):
        try:
            # Try to read existing audit table
            spark.read.format("delta").load(self.audit_path)
        except:
            # Create new audit table
            empty_audit = spark.createDataFrame([], self.get_audit_schema())
            empty_audit.write.format("delta").save(self.audit_path)
    
    def log_operation(self, table_name, operation, details):
        audit_entry = spark.createDataFrame([{
            "table_name": table_name,
            "operation": operation,
            "timestamp": datetime.now(),
            "user": current_user(),
            "details": str(details),
            "version_before": self.get_table_version(table_name),
            "session_id": spark.sparkContext.applicationId
        }])
        
        audit_entry.write.format("delta").mode("append").save(self.audit_path)
```

---

### Q11: No of Records per Partition - spark_partition_Id

**Answer:**

The `spark_partition_id()` function returns the partition ID for each row, enabling analysis of data distribution across partitions.

**Function Usage:**
```python
from pyspark.sql.functions import spark_partition_id, count

# Add partition ID to DataFrame
df_with_partition = df.withColumn("partition_id", spark_partition_id())

# Count records per partition
partition_counts = df_with_partition.groupBy("partition_id").count().orderBy("partition_id")
partition_counts.show()
```

**Detailed Analysis Examples:**

**1. Basic Partition Analysis:**
```python
# Get partition distribution
def analyze_partition_distribution(df):
    partition_stats = df.withColumn("partition_id", spark_partition_id()) \
                       .groupBy("partition_id") \
                       .agg(count("*").alias("record_count")) \
                       .orderBy("partition_id")
    
    partition_stats.show()
    
    # Get statistics
    total_partitions = partition_stats.count()
    total_records = df.count()
    avg_records_per_partition = total_records / total_partitions
    
    print(f"Total Partitions: {total_partitions}")
    print(f"Total Records: {total_records}")
    print(f"Average Records per Partition: {avg_records_per_partition}")
    
    return partition_stats

# Usage
partition_analysis = analyze_partition_distribution(df)
```

**2. Identify Data Skewness:**
```python
from pyspark.sql.functions import max, min, stddev, avg

def detect_partition_skewness(df):
    partition_counts = df.withColumn("partition_id", spark_partition_id()) \
                        .groupBy("partition_id") \
                        .count()
    
    skewness_stats = partition_counts.agg(
        min("count").alias("min_records"),
        max("count").alias("max_records"),
        avg("count").alias("avg_records"),
        stddev("count").alias("stddev_records")
    ).collect()[0]
    
    # Calculate skewness ratio
    skewness_ratio = skewness_stats["max_records"] / skewness_stats["avg_records"]
    
    print(f"Min records per partition: {skewness_stats['min_records']}")
    print(f"Max records per partition: {skewness_stats['max_records']}")
    print(f"Avg records per partition: {skewness_stats['avg_records']:.2f}")
    print(f"Skewness ratio: {skewness_ratio:.2f}")
    
    if skewness_ratio > 2:
        print("⚠️  High data skewness detected!")
    
    return skewness_stats
```

**3. Partition Size Analysis:**
```python
import pyspark.sql.functions as F

def analyze_partition_sizes(df):
    # Estimate partition sizes (approximate)
    partition_analysis = df.withColumn("partition_id", spark_partition_id()) \
                           .withColumn("row_size_estimate", 
                                     F.length(F.to_json(F.struct(*df.columns)))) \
                           .groupBy("partition_id") \
                           .agg(
                               F.count("*").alias("record_count"),
                               F.sum("row_size_estimate").alias("total_size_bytes"),
                               F.avg("row_size_estimate").alias("avg_row_size")
                           )
    
    return partition_analysis
```

**4. Optimize Partitioning:**
```python
def optimize_partitioning(df, target_partition_size_mb=128):
    # Calculate current distribution
    current_partitions = df.rdd.getNumPartitions()
    total_size_mb = df.rdd.map(lambda x: len(str(x))).sum() / (1024 * 1024)
    
    # Calculate optimal partition count
    optimal_partitions = max(1, int(total_size_mb / target_partition_size_mb))
    
    print(f"Current partitions: {current_partitions}")
    print(f"Estimated total size: {total_size_mb:.2f} MB")
    print(f"Recommended partitions: {optimal_partitions}")
    
    if optimal_partitions != current_partitions:
        if optimal_partitions < current_partitions:
            return df.coalesce(optimal_partitions)
        else:
            return df.repartition(optimal_partitions)
    
    return df
```

---

### Q12: Data Skewness

**Answer:**

Data skewness occurs when data is unevenly distributed across partitions, causing performance bottlenecks and resource inefficiency.

**Types of Skewness:**

**1. Partition Skewness:**
- Uneven data distribution across partitions
- Some partitions have significantly more data

**2. Key Skewness:**
- Uneven distribution of values for join/group keys
- Hot keys with disproportionate data

**Detection Methods:**

**1. Partition-Level Skewness Detection:**
```python
from pyspark.sql.functions import spark_partition_id, count, col

def detect_partition_skewness(df):
    partition_counts = df.withColumn("partition_id", spark_partition_id()) \
                        .groupBy("partition_id") \
                        .count() \
                        .collect()
    
    counts = [row['count'] for row in partition_counts]
    avg_count = sum(counts) / len(counts)
    max_count = max(counts)
    min_count = min(counts)
    
    skew_ratio = max_count / avg_count if avg_count > 0 else 0
    
    print(f"Partition count range: {min_count} - {max_count}")
    print(f"Average: {avg_count:.2f}")
    print(f"Skew ratio: {skew_ratio:.2f}")
    
    return skew_ratio > 2  # Threshold for skewness
```

**2. Key-Level Skewness Detection:**
```python
def detect_key_skewness(df, key_column, threshold=0.1):
    total_count = df.count()
    key_distribution = df.groupBy(key_column).count() \
                        .withColumn("percentage", col("count") / total_count) \
                        .orderBy(col("count").desc())
    
    # Show top keys
    key_distribution.show(20)
    
    # Check if any key exceeds threshold
    hot_keys = key_distribution.filter(col("percentage") > threshold)
    
    if hot_keys.count() > 0:
        print(f"⚠️  Hot keys detected (>{threshold*100}% of data):")
        hot_keys.show()
        return True
    
    return False
```

**Mitigation Strategies:**

**1. Salting Technique:**
```python
from pyspark.sql.functions import rand, floor

def salt_skewed_keys(df, skewed_key, salt_buckets=10):
    # Add salt to skewed keys
    salted_df = df.withColumn("salt", floor(rand() * salt_buckets)) \
                  .withColumn("salted_key", 
                            concat(col(skewed_key), lit("_"), col("salt")))
    
    return salted_df

# Usage for joins
def salted_join(left_df, right_df, join_key, salt_buckets=10):
    # Salt both DataFrames
    left_salted = salt_skewed_keys(left_df, join_key, salt_buckets)
    
    # Replicate right DataFrame for each salt value
    salt_values = range(salt_buckets)
    right_replicated = None
    
    for salt in salt_values:
        right_with_salt = right_df.withColumn("salt", lit(salt)) \
                                 .withColumn("salted_key", 
                                           concat(col(join_key), lit("_"), lit(salt)))
        if right_replicated is None:
            right_replicated = right_with_salt
        else:
            right_replicated = right_replicated.union(right_with_salt)
    
    # Perform join on salted key
    result = left_salted.join(right_replicated, "salted_key") \
                       .drop("salt", "salted_key")
    
    return result
```

**2. Repartitioning Strategies:**
```python
# Range partitioning for ordered data
df_range_partitioned = df.repartitionByRange(10, "timestamp")

# Hash partitioning for even distribution
df_hash_partitioned = df.repartition(20, "customer_id")

# Custom partitioning function
def custom_partition_key(row):
    # Custom logic to create balanced partitions
    return hash(row.key) % 50

# Use custom partitioner
df_custom_partitioned = df.rdd.partitionBy(50, custom_partition_key).toDF()
```

**3. Broadcast Joins for Small Tables:**
```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension table
result = large_fact_table.join(broadcast(small_dim_table), "key")
```

**4. Bucketing for Repeated Joins:**
```python
# Pre-bucket tables for efficient joins
df.write.bucketBy(20, "join_key").saveAsTable("bucketed_table")
```

---

## 13. Azure Storage Types

### Q13: How many types of storage in Azure, what are they and where they are used

**Answer:**

Azure provides multiple storage services, each optimized for specific use cases and access patterns.

**1. Azure Blob Storage**
- **Purpose:** Object storage for unstructured data
- **Use Cases:** 
  - Backup and restore
  - Data archiving
  - Content distribution
  - Big data analytics
  - Static website hosting
- **Tiers:** Hot, Cool, Archive

**2. Azure Files**
- **Purpose:** Managed file shares using SMB protocol
- **Use Cases:**
  - Shared application settings
  - File sharing across VMs
  - Lift-and-shift scenarios
  - Development tools and debugging

**3. Azure Queue Storage**
- **Purpose:** Message queuing service
- **Use Cases:**
  - Decoupling application components
  - Asynchronous processing
  - Work distribution
  - Building resilient applications

**4. Azure Table Storage**
- **Purpose:** NoSQL key-value store
- **Use Cases:**
  - Web application data
  - Address books
  - Device information
  - Metadata storage

**5. Azure Disk Storage**
- **Purpose:** Block-level storage for Azure VMs
- **Types:**
  - Ultra Disk: Highest performance
  - Premium SSD: High performance, low latency
  - Standard SSD: Balanced performance
  - Standard HDD: Cost-effective for infrequent access

**6. Azure Data Lake Storage (ADLS)**
- **Purpose:** Big data analytics storage
- **Features:**
  - Hierarchical namespace
  - Hadoop compatibility
  - Fine-grained security
  - Optimized for analytics workloads

**7. Azure NetApp Files**
- **Purpose:** Enterprise-grade NFS and SMB file shares
- **Use Cases:**
  - High-performance computing
  - Database workloads
  - Enterprise applications

**Storage Service Comparison:**

| Service | Type | Protocol | Use Case | Performance |
|---------|------|----------|----------|-------------|
| Blob | Object | REST/HTTP | Unstructured data | High throughput |
| Files | File | SMB/NFS | Shared files | Moderate |
| Queue | Message | REST | Messaging | Message-optimized |
| Table | NoSQL | REST | Key-value data | Fast queries |
| Disk | Block | SCSI | VM storage | Various tiers |
| ADLS | Object+ | REST/HDFS | Big data | Analytics-optimized |

---

This covers questions 9-13. Would you like me to continue with the remaining questions (14-40) to complete the comprehensive guide? 
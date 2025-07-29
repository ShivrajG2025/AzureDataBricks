# Comprehensive Data Engineering Guide

## Table of Contents

1. [Databricks Workflows](#databricks-workflows)
2. [Handling Corrupt Records in Databricks](#handling-corrupt-records)
3. [Compression: Snappy vs Gzip](#compression-comparison)
4. [Slowly Changing Dimensions (SCD)](#slowly-changing-dimensions)
5. [Cross Join vs Full Outer Join](#join-differences)
6. [SCD1 vs SCD2 Differences](#scd-types)
7. [Parameters and Variables in ADF](#adf-parameters)
8. [Data Warehouse & ETL](#datawarehouse-etl)
9. [Fact Tables and Dimension Tables](#fact-dimension-tables)
10. [Azure Synapse](#azure-synapse)
11. [Streaming vs Batch Processing & Delta Tables](#streaming-batch)
12. [SCD2 Implementation using Databricks PySpark and ADF](#scd2-implementation)
13. [Calling Stored Procedures in ADF](#stored-procedures-adf)
14. [Parallelism in Databricks](#databricks-parallelism)
15. [Compression Types for Parquet Files](#parquet-compression)
16. [Additional Columns in ADF Copy Activity](#adf-additional-columns)
17. [System Variables in ADF Pipeline](#adf-system-variables)

---

## 1. Databricks Workflows {#databricks-workflows}

### Overview
Databricks Workflows (formerly Jobs) are a way to schedule and orchestrate data processing tasks in Databricks. They allow you to run notebooks, JAR files, Python scripts, and other tasks on a schedule or trigger.

### Key Components
- **Jobs**: The main workflow entity
- **Tasks**: Individual units of work within a job
- **Clusters**: Compute resources to run tasks
- **Triggers**: Schedule or event-based execution

### Scenario: Daily ETL Pipeline

**Business Requirement**: Process daily sales data, transform it, and load into a data warehouse.

**Implementation**:

#### Step 1: Create Job Configuration (JSON)
```json
{
  "name": "Daily_Sales_ETL_Pipeline",
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "extract_raw_data",
      "notebook_task": {
        "notebook_path": "/notebooks/extract_sales_data",
        "base_parameters": {
          "date": "{{job.start_time}}"
        }
      },
      "job_cluster_key": "etl_cluster",
      "timeout_seconds": 3600
    },
    {
      "task_key": "transform_data",
      "depends_on": [
        {
          "task_key": "extract_raw_data"
        }
      ],
      "notebook_task": {
        "notebook_path": "/notebooks/transform_sales_data"
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "load_to_warehouse",
      "depends_on": [
        {
          "task_key": "transform_data"
        }
      ],
      "notebook_task": {
        "notebook_path": "/notebooks/load_sales_data"
      },
      "job_cluster_key": "etl_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "11.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

#### Step 2: Extract Notebook (Python)
```python
# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Get parameters
dbutils.widgets.text("date", "")
process_date = dbutils.widgets.get("date")

# Define schema for sales data
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("store_id", StringType(), True)
])

# Read raw data
raw_sales_df = spark.read \
    .option("header", "true") \
    .schema(sales_schema) \
    .csv(f"/mnt/raw-data/sales/{process_date}")

# Write to bronze layer
raw_sales_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("bronze.raw_sales")

print(f"Extracted {raw_sales_df.count()} records for {process_date}")
```

#### Step 3: Transform Notebook (Python)
```python
# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Read from bronze layer
bronze_df = spark.table("bronze.raw_sales")

# Data quality checks and transformations
silver_df = bronze_df \
    .filter(F.col("quantity") > 0) \
    .filter(F.col("unit_price") > 0) \
    .withColumn("total_amount", F.col("quantity") * F.col("unit_price")) \
    .withColumn("revenue_category", 
                F.when(F.col("total_amount") > 1000, "High")
                 .when(F.col("total_amount") > 100, "Medium")
                 .otherwise("Low")) \
    .withColumn("processed_timestamp", F.current_timestamp())

# Write to silver layer
silver_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.processed_sales")

print(f"Transformed {silver_df.count()} records")
```

### Workflow Monitoring and Alerting

```python
# Email notification configuration
{
  "email_notifications": {
    "on_start": ["data-team@company.com"],
    "on_success": ["data-team@company.com"],
    "on_failure": ["data-team@company.com", "ops-team@company.com"]
  }
}
```

### Best Practices
1. **Task Dependencies**: Use `depends_on` to create proper task sequences
2. **Error Handling**: Implement retry logic and failure notifications
3. **Resource Management**: Use job clusters for cost optimization
4. **Monitoring**: Set up proper logging and alerting
5. **Parameters**: Use widgets for dynamic parameter passing

---

## 2. Handling Corrupt Records in Databricks {#handling-corrupt-records}

### Overview
Corrupt records are malformed data that cannot be parsed according to the expected schema. Databricks provides several strategies to handle such records.

### Methods to Handle Corrupt Records

#### Method 1: Using `input_file_name()` with PERMISSIVE Mode

**Scenario**: Processing CSV files where some records might be malformed.

```python
# Define schema with _corrupt_record column
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name, current_timestamp

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)  # This will capture corrupt records
])

# Read with PERMISSIVE mode (default)
df = spark.read \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .schema(schema) \
    .csv("/path/to/csv/files") \
    .withColumn("source_file", input_file_name()) \
    .withColumn("load_timestamp", current_timestamp())

# Separate good and bad records
good_records = df.filter(df._corrupt_record.isNull()).drop("_corrupt_record")
corrupt_records = df.filter(df._corrupt_record.isNotNull())

# Process good records
good_records.write.mode("append").saveAsTable("processed_data")

# Log corrupt records for investigation
corrupt_records.select("source_file", "_corrupt_record", "load_timestamp") \
    .write.mode("append").saveAsTable("corrupt_records_log")

print(f"Good records: {good_records.count()}")
print(f"Corrupt records: {corrupt_records.count()}")
```

#### Method 2: Using Different Parse Modes

```python
# FAILFAST mode - stops on first corrupt record
df_failfast = spark.read \
    .option("mode", "FAILFAST") \
    .option("header", "true") \
    .csv("/path/to/csv/files")

# DROPMALFORMED mode - drops corrupt records
df_drop = spark.read \
    .option("mode", "DROPMALFORMED") \
    .option("header", "true") \
    .csv("/path/to/csv/files")

# PERMISSIVE mode - keeps corrupt records in _corrupt_record column
df_permissive = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("header", "true") \
    .csv("/path/to/csv/files")
```

#### Method 3: Custom Corrupt Record Handling

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

def handle_corrupt_records(df, table_name):
    """
    Custom function to handle corrupt records with detailed logging
    """
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("source_file", input_file_name()) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("file_modification_time", 
                   to_timestamp(regexp_extract(input_file_name(), r'(\d{8})', 1), 'yyyyMMdd'))
    
    # Separate records
    valid_records = df_with_metadata.filter(col("_corrupt_record").isNull())
    invalid_records = df_with_metadata.filter(col("_corrupt_record").isNotNull())
    
    # Process valid records
    clean_data = valid_records.drop("_corrupt_record")
    
    # Create detailed corrupt record report
    corrupt_report = invalid_records.select(
        col("source_file"),
        col("_corrupt_record").alias("corrupt_data"),
        col("processing_timestamp"),
        col("file_modification_time"),
        length(col("_corrupt_record")).alias("record_length"),
        regexp_extract(col("_corrupt_record"), r'^([^,]*)', 1).alias("first_field")
    )
    
    # Write to respective tables
    clean_data.write.mode("append").saveAsTable(f"{table_name}_clean")
    corrupt_report.write.mode("append").saveAsTable(f"{table_name}_corrupt_log")
    
    return {
        "valid_count": clean_data.count(),
        "corrupt_count": corrupt_report.count(),
        "success_rate": clean_data.count() / (clean_data.count() + corrupt_report.count()) * 100
    }

# Usage
result = handle_corrupt_records(df, "customer_data")
print(f"Processing Summary: {result}")
```

### Advanced Corrupt Record Analysis

```python
# Analyze patterns in corrupt records
corrupt_analysis = spark.sql("""
    SELECT 
        source_file,
        COUNT(*) as corrupt_count,
        MIN(record_length) as min_length,
        MAX(record_length) as max_length,
        AVG(record_length) as avg_length,
        COUNT(DISTINCT first_field) as unique_first_fields,
        processing_timestamp
    FROM customer_data_corrupt_log
    GROUP BY source_file, processing_timestamp
    ORDER BY corrupt_count DESC
""")

corrupt_analysis.show(truncate=False)
```

### Recovery Strategies

```python
# Strategy 1: Attempt to fix common issues
def attempt_record_recovery(corrupt_df):
    """
    Try to recover some corrupt records by fixing common issues
    """
    
    # Try to fix records with extra commas
    fixed_df = corrupt_df.withColumn(
        "attempted_fix",
        regexp_replace(col("_corrupt_record"), ",+", ",")
    )
    
    # Try to parse the fixed records
    # This would require custom parsing logic based on your data format
    
    return fixed_df

# Strategy 2: Manual review queue
def create_manual_review_queue(corrupt_df):
    """
    Create a queue for manual review of corrupt records
    """
    
    review_queue = corrupt_df.select(
        col("source_file"),
        col("_corrupt_record"),
        col("processing_timestamp"),
        lit("PENDING").alias("review_status"),
        lit(None).alias("reviewer"),
        lit(None).alias("resolution"),
        current_timestamp().alias("queue_timestamp")
    )
    
    review_queue.write.mode("append").saveAsTable("manual_review_queue")
    
    return review_queue.count()
```

---

## 3. Compression: Snappy vs Gzip {#compression-comparison}

### Overview
Compression is crucial for data storage and processing efficiency. Snappy and Gzip are two popular compression algorithms with different trade-offs.

### Detailed Comparison

| Aspect | Snappy | Gzip |
|--------|--------|------|
| **Compression Speed** | Very Fast | Slow |
| **Decompression Speed** | Very Fast | Fast |
| **Compression Ratio** | Lower | Higher |
| **CPU Usage** | Low | High |
| **Best Use Case** | Real-time processing | Long-term storage |
| **Splittable** | Yes (with proper format) | No |

### Scenario-Based Implementations

#### Scenario 1: Real-time Analytics Pipeline

**Requirement**: Process streaming data with low latency.

```python
# Using Snappy for real-time processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("RealTimeAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configure for Snappy compression
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Read streaming data
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

# Process and write with Snappy compression
processed_stream = streaming_df \
    .select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp())

# Write to Delta table with Snappy compression
query = processed_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("compression", "snappy") \
    .outputMode("append") \
    .table("real_time_events")

query.start().awaitTermination()
```

#### Scenario 2: Data Archival and Long-term Storage

**Requirement**: Store historical data with maximum space efficiency.

```python
# Using Gzip for archival storage
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Configure for Gzip compression
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

# Read historical data
historical_df = spark.read \
    .option("multiline", "true") \
    .json("/path/to/large/json/files")

# Perform aggregations for archival
archived_data = historical_df \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .groupBy("year", "month", "category") \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("record_count"),
        avg("amount").alias("avg_amount"),
        max("timestamp").alias("last_updated")
    )

# Write with Gzip compression for long-term storage
archived_data.write \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .partitionBy("year", "month") \
    .parquet("/path/to/archive/data")

print("Data archived with Gzip compression")
```

### Performance Testing Framework

```python
import time
from pyspark.sql.functions import *

def compression_performance_test(df, compression_types=["snappy", "gzip", "lz4"]):
    """
    Test different compression algorithms for performance
    """
    results = {}
    
    for compression in compression_types:
        print(f"Testing {compression} compression...")
        
        # Set compression
        spark.conf.set("spark.sql.parquet.compression.codec", compression)
        
        # Write test
        start_time = time.time()
        df.write \
            .mode("overwrite") \
            .option("compression", compression) \
            .parquet(f"/tmp/test_{compression}")
        write_time = time.time() - start_time
        
        # Read test
        start_time = time.time()
        test_df = spark.read.parquet(f"/tmp/test_{compression}")
        test_df.count()  # Force action
        read_time = time.time() - start_time
        
        # Get file size
        file_size = dbutils.fs.ls(f"/tmp/test_{compression}")[0].size
        
        results[compression] = {
            "write_time": write_time,
            "read_time": read_time,
            "file_size_mb": file_size / (1024 * 1024),
            "total_time": write_time + read_time
        }
    
    return results

# Test with sample data
sample_df = spark.range(1000000).select(
    col("id"),
    (col("id") * 1.5).alias("value"),
    date_add(current_date(), (col("id") % 365).cast("int")).alias("date"),
    concat(lit("user_"), col("id")).alias("user_name")
)

performance_results = compression_performance_test(sample_df)

# Display results
for compression, metrics in performance_results.items():
    print(f"{compression.upper()}:")
    print(f"  Write Time: {metrics['write_time']:.2f}s")
    print(f"  Read Time: {metrics['read_time']:.2f}s")
    print(f"  File Size: {metrics['file_size_mb']:.2f} MB")
    print(f"  Total Time: {metrics['total_time']:.2f}s")
    print()
```

### Use Case Decision Matrix

```python
def recommend_compression(use_case_params):
    """
    Recommend compression based on use case parameters
    """
    
    score_snappy = 0
    score_gzip = 0
    
    # Scoring based on different factors
    if use_case_params.get("read_frequency", "low") == "high":
        score_snappy += 3
    elif use_case_params.get("read_frequency", "low") == "medium":
        score_snappy += 1
        score_gzip += 1
    else:
        score_gzip += 2
    
    if use_case_params.get("storage_duration", "short") == "long":
        score_gzip += 3
    else:
        score_snappy += 2
    
    if use_case_params.get("processing_type", "batch") == "streaming":
        score_snappy += 4
    else:
        score_gzip += 1
    
    if use_case_params.get("cpu_resources", "high") == "limited":
        score_snappy += 2
    else:
        score_gzip += 1
    
    if use_case_params.get("storage_cost_priority", "low") == "high":
        score_gzip += 3
    else:
        score_snappy += 1
    
    recommendation = "snappy" if score_snappy > score_gzip else "gzip"
    confidence = abs(score_snappy - score_gzip) / max(score_snappy, score_gzip) * 100
    
    return {
        "recommendation": recommendation,
        "confidence": f"{confidence:.1f}%",
        "snappy_score": score_snappy,
        "gzip_score": score_gzip,
        "reasoning": {
            "snappy_advantages": ["Fast compression/decompression", "Low CPU usage", "Good for streaming"],
            "gzip_advantages": ["Better compression ratio", "Lower storage costs", "Good for archival"]
        }
    }

# Example usage
use_case = {
    "read_frequency": "high",        # high, medium, low
    "storage_duration": "short",     # short, medium, long
    "processing_type": "streaming",  # streaming, batch
    "cpu_resources": "high",         # high, medium, limited
    "storage_cost_priority": "low"   # high, medium, low
}

recommendation = recommend_compression(use_case)
print(f"Recommended compression: {recommendation['recommendation']}")
print(f"Confidence: {recommendation['confidence']}")
```

---

## 4. Slowly Changing Dimensions (SCD) {#slowly-changing-dimensions}

### Overview
Slowly Changing Dimensions (SCD) are techniques used in data warehousing to manage and track changes in dimension data over time.

### Types of SCD

#### SCD Type 0: No Changes
- No updates are allowed
- Data is static and historical

#### SCD Type 1: Overwrite
- Overwrites old data with new data
- No history is maintained

#### SCD Type 2: Add New Record
- Maintains full history by adding new records
- Uses effective dates and flags

#### SCD Type 3: Add New Attribute
- Maintains limited history by adding columns
- Tracks only current and previous values

### Scenario: Customer Dimension Management

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# Sample customer data
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Initial customer data
initial_customers = spark.createDataFrame([
    ("C001", "John Smith", "john@email.com", "123-456-7890", "123 Main St", "New York", "NY", "2023-01-01 00:00:00"),
    ("C002", "Jane Doe", "jane@email.com", "987-654-3210", "456 Oak Ave", "Los Angeles", "CA", "2023-01-01 00:00:00"),
    ("C003", "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", "Chicago", "IL", "2023-01-01 00:00:00")
], customer_schema)

# Create initial Delta table
initial_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_customer_scd1")

print("Initial customer dimension created")
```

### SCD Type 1 Implementation

```python
def scd_type1_update(target_table, source_df, key_column):
    """
    Implement SCD Type 1 - Overwrite existing records
    """
    
    # Read current dimension
    current_dim = spark.table(target_table)
    
    # Identify changes
    changes = source_df.alias("source") \
        .join(current_dim.alias("target"), key_column, "inner") \
        .where(
            (col("source.name") != col("target.name")) |
            (col("source.email") != col("target.email")) |
            (col("source.phone") != col("target.phone")) |
            (col("source.address") != col("target.address")) |
            (col("source.city") != col("target.city")) |
            (col("source.state") != col("target.state"))
        ) \
        .select("source.*")
    
    # Get new records
    new_records = source_df.alias("source") \
        .join(current_dim.alias("target"), key_column, "left_anti")
    
    # Perform merge operation
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Update existing records
    if changes.count() > 0:
        delta_table.alias("target") \
            .merge(changes.alias("source"), f"target.{key_column} = source.{key_column}") \
            .whenMatchedUpdateAll() \
            .execute()
        
        print(f"Updated {changes.count()} existing records")
    
    # Insert new records
    if new_records.count() > 0:
        new_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)
        
        print(f"Inserted {new_records.count()} new records")

# Example usage with updated data
updated_customers = spark.createDataFrame([
    ("C001", "John Smith Jr.", "john.jr@email.com", "123-456-7890", "123 Main St", "New York", "NY", "2023-06-01 00:00:00"),
    ("C002", "Jane Doe", "jane@email.com", "987-654-3210", "789 New Ave", "San Francisco", "CA", "2023-06-01 00:00:00"),
    ("C004", "Alice Brown", "alice@email.com", "444-555-6666", "321 Elm St", "Boston", "MA", "2023-06-01 00:00:00")
], customer_schema)

scd_type1_update("dim_customer_scd1", updated_customers, "customer_id")
```

### SCD Type 2 Implementation

```python
# Enhanced schema for SCD Type 2
scd2_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("record_hash", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

def create_scd2_dimension(source_df, target_table):
    """
    Create initial SCD Type 2 dimension table
    """
    
    scd2_df = source_df \
        .withColumn("effective_date", current_date()) \
        .withColumn("end_date", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("version", lit(1)) \
        .withColumn("record_hash", 
                   sha2(concat_ws("|", col("name"), col("email"), col("phone"), 
                                col("address"), col("city"), col("state")), 256)) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("last_updated", current_timestamp())
    
    scd2_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
    
    print(f"Created SCD Type 2 dimension with {scd2_df.count()} records")

def scd_type2_update(target_table, source_df, key_column, effective_date):
    """
    Implement SCD Type 2 - Add new records for changes
    """
    
    # Add hash to source data
    source_with_hash = source_df \
        .withColumn("record_hash", 
                   sha2(concat_ws("|", col("name"), col("email"), col("phone"), 
                                col("address"), col("city"), col("state")), 256))
    
    # Get current active records
    current_active = spark.table(target_table).filter(col("is_current") == True)
    
    # Identify changes
    changes = source_with_hash.alias("source") \
        .join(current_active.alias("target"), key_column, "inner") \
        .where(col("source.record_hash") != col("target.record_hash")) \
        .select("source.*", col("target.version").alias("current_version"))
    
    # Identify new records
    new_records = source_with_hash.alias("source") \
        .join(current_active.alias("target"), key_column, "left_anti")
    
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Process changes
    if changes.count() > 0:
        # Close existing records
        delta_table.alias("target") \
            .merge(changes.alias("source"), 
                  f"target.{key_column} = source.{key_column} AND target.is_current = true") \
            .whenMatchedUpdate(set = {
                "end_date": lit(effective_date),
                "is_current": lit(False),
                "last_updated": current_timestamp()
            }) \
            .execute()
        
        # Insert new versions
        new_versions = changes \
            .withColumn("effective_date", lit(effective_date)) \
            .withColumn("end_date", lit(None).cast(DateType())) \
            .withColumn("is_current", lit(True)) \
            .withColumn("version", col("current_version") + 1) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("last_updated", current_timestamp()) \
            .drop("current_version")
        
        new_versions.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)
        
        print(f"Processed {changes.count()} changed records")
    
    # Insert completely new records
    if new_records.count() > 0:
        new_customer_records = new_records \
            .withColumn("effective_date", lit(effective_date)) \
            .withColumn("end_date", lit(None).cast(DateType())) \
            .withColumn("is_current", lit(True)) \
            .withColumn("version", lit(1)) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("last_updated", current_timestamp())
        
        new_customer_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)
        
        print(f"Inserted {new_records.count()} new customer records")

# Create initial SCD Type 2 dimension
create_scd2_dimension(initial_customers, "dim_customer_scd2")

# Apply updates
scd_type2_update("dim_customer_scd2", updated_customers, "customer_id", "2023-06-01")

# Query SCD Type 2 data
scd2_results = spark.sql("""
    SELECT 
        customer_id,
        name,
        email,
        city,
        state,
        effective_date,
        end_date,
        is_current
    FROM dim_customer_scd2
    ORDER BY customer_id, effective_date
""")

scd2_results.show(truncate=False)
```

---

## 5. Cross Join vs Full Outer Join {#join-differences}

### Overview
Understanding the differences between Cross Join and Full Outer Join is crucial for data engineering and SQL operations.

### Cross Join

**Definition**: Cartesian product of two datasets - every row from the first dataset is combined with every row from the second dataset.

**Scenario**: Creating a comprehensive sales report template.

```python
# Sample data
products_data = [("P001", "Laptop"), ("P002", "Mouse"), ("P003", "Keyboard")]
stores_data = [("S001", "New York"), ("S002", "Los Angeles"), ("S003", "Chicago")]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
stores_df = spark.createDataFrame(stores_data, ["store_id", "store_name"])

# Cross Join - Creates all possible combinations
cross_join_result = products_df.crossJoin(stores_df)

print("Cross Join Result:")
cross_join_result.show()

# Result: 3 products × 3 stores = 9 rows
# Every product appears with every store
```

**Output**:
```
+----------+------------+--------+----------+
|product_id|product_name|store_id|store_name|
+----------+------------+--------+----------+
|      P001|      Laptop|    S001|  New York|
|      P001|      Laptop|    S002|Los Angeles|
|      P001|      Laptop|    S003|   Chicago|
|      P002|       Mouse|    S001|  New York|
|      P002|       Mouse|    S002|Los Angeles|
|      P002|       Mouse|    S003|   Chicago|
|      P003|    Keyboard|    S001|  New York|
|      P003|    Keyboard|    S002|Los Angeles|
|      P003|    Keyboard|    S003|   Chicago|
+----------+------------+--------+----------+
```

### Full Outer Join

**Definition**: Returns all rows from both datasets, with NULLs where no match exists.

**Scenario**: Reconciling customer data from two different systems.

```python
# Sample customer data from two systems
system1_data = [
    ("C001", "John Smith", "john@email.com"),
    ("C002", "Jane Doe", "jane@email.com"),
    ("C003", "Bob Johnson", "bob@email.com")
]

system2_data = [
    ("C002", "Jane Doe", "555-1234"),
    ("C003", "Robert Johnson", "555-5678"),
    ("C004", "Alice Brown", "555-9012")
]

system1_df = spark.createDataFrame(system1_data, ["customer_id", "name", "email"])
system2_df = spark.createDataFrame(system2_data, ["customer_id", "name", "phone"])

# Full Outer Join - Includes all records from both datasets
full_outer_result = system1_df.join(system2_df, "customer_id", "full_outer")

print("Full Outer Join Result:")
full_outer_result.show()

# Enhanced full outer join with indicators
full_outer_enhanced = system1_df.alias("s1") \
    .join(system2_df.alias("s2"), "customer_id", "full_outer") \
    .select(
        coalesce(col("s1.customer_id"), col("s2.customer_id")).alias("customer_id"),
        col("s1.name").alias("system1_name"),
        col("s1.email"),
        col("s2.name").alias("system2_name"),
        col("s2.phone"),
        when(col("s1.customer_id").isNotNull() & col("s2.customer_id").isNotNull(), "Both")
        .when(col("s1.customer_id").isNotNull(), "System1 Only")
        .otherwise("System2 Only").alias("source_indicator")
    )

print("Enhanced Full Outer Join with Source Indicators:")
full_outer_enhanced.show(truncate=False)
```

### Practical Comparison Example

```python
def demonstrate_join_differences():
    """
    Comprehensive demonstration of Cross Join vs Full Outer Join
    """
    
    # Sample sales data
    actual_sales = [
        ("P001", "S001", 100),
        ("P001", "S002", 150),
        ("P002", "S001", 75),
        ("P003", "S003", 200)
    ]
    
    actual_sales_df = spark.createDataFrame(
        actual_sales, 
        ["product_id", "store_id", "sales_amount"]
    )
    
    # Create template using Cross Join (all possible combinations)
    sales_template = products_df.crossJoin(stores_df) \
        .select("product_id", "store_id") \
        .withColumn("template_flag", lit(True))
    
    print("Sales Template (Cross Join):")
    sales_template.show()
    
    # Full Outer Join to see actual vs expected sales
    sales_analysis = sales_template.join(
        actual_sales_df, 
        ["product_id", "store_id"], 
        "full_outer"
    ).select(
        "product_id",
        "store_id", 
        coalesce(col("sales_amount"), lit(0)).alias("sales_amount"),
        when(col("template_flag").isNotNull() & col("sales_amount").isNotNull(), "Has Sales")
        .when(col("template_flag").isNotNull(), "No Sales")
        .otherwise("Unexpected Sale").alias("status")
    )
    
    print("Sales Analysis (Full Outer Join):")
    sales_analysis.orderBy("product_id", "store_id").show()
    
    # Summary statistics
    summary = sales_analysis.groupBy("status").agg(
        count("*").alias("count"),
        sum("sales_amount").alias("total_sales")
    )
    
    print("Summary by Status:")
    summary.show()

demonstrate_join_differences()
```

### Performance Considerations

```python
def join_performance_comparison():
    """
    Compare performance characteristics of different joins
    """
    
    # Create larger datasets for performance testing
    large_products = spark.range(1000).select(
        concat(lit("P"), col("id").cast("string")).alias("product_id"),
        concat(lit("Product "), col("id")).alias("product_name")
    )
    
    large_stores = spark.range(100).select(
        concat(lit("S"), col("id").cast("string")).alias("store_id"),
        concat(lit("Store "), col("id")).alias("store_name")
    )
    
    print("Performance Comparison:")
    
    # Cross Join performance
    import time
    start_time = time.time()
    cross_result = large_products.crossJoin(large_stores)
    cross_count = cross_result.count()
    cross_time = time.time() - start_time
    
    print(f"Cross Join: {cross_count} rows in {cross_time:.2f} seconds")
    
    # Full Outer Join performance (with some matching data)
    sales_data = spark.range(5000).select(
        concat(lit("P"), (col("id") % 1000).cast("string")).alias("product_id"),
        concat(lit("S"), (col("id") % 100).cast("string")).alias("store_id"),
        (col("id") * 10).alias("sales_amount")
    )
    
    start_time = time.time()
    full_outer_result = large_products.join(sales_data, "product_id", "full_outer")
    full_outer_count = full_outer_result.count()
    full_outer_time = time.time() - start_time
    
    print(f"Full Outer Join: {full_outer_count} rows in {full_outer_time:.2f} seconds")
    
    return {
        "cross_join": {"count": cross_count, "time": cross_time},
        "full_outer_join": {"count": full_outer_count, "time": full_outer_time}
    }

# Run performance comparison
# performance_results = join_performance_comparison()
```

### When to Use Each Join

#### Cross Join Use Cases:
1. **Creating Templates**: All possible combinations for reporting
2. **Calendar Tables**: Every date with every dimension
3. **Cartesian Products**: Mathematical operations requiring all combinations
4. **Testing Scenarios**: Generate test data combinations

#### Full Outer Join Use Cases:
1. **Data Reconciliation**: Comparing datasets from different sources
2. **Missing Data Analysis**: Finding gaps in data
3. **Data Completeness**: Ensuring all expected records exist
4. **Master Data Management**: Consolidating multiple data sources

---

## 6. SCD1 vs SCD2 Differences {#scd-types}

### Comprehensive Comparison

| Aspect | SCD Type 1 | SCD Type 2 |
|--------|------------|------------|
| **History Tracking** | No | Yes |
| **Storage Space** | Minimal | Higher |
| **Complexity** | Simple | Complex |
| **Query Performance** | Fast | Slower |
| **Use Case** | Current state only | Historical analysis |
| **Data Volume** | Single row per entity | Multiple rows per entity |

### Detailed Implementation Comparison

#### Business Scenario: Employee Management System

```python
# Initial employee data
employee_data = [
    ("E001", "John Smith", "Developer", "IT", 75000, "2020-01-01"),
    ("E002", "Jane Doe", "Manager", "Sales", 85000, "2019-05-15"),
    ("E003", "Bob Johnson", "Analyst", "Finance", 65000, "2021-03-10")
]

employee_schema = StructType([
    StructField("employee_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", StringType(), True)
])

initial_employees = spark.createDataFrame(employee_data, employee_schema)

# Updated employee data (promotions and raises)
updated_employee_data = [
    ("E001", "John Smith", "Senior Developer", "IT", 85000, "2020-01-01"),  # Promotion
    ("E002", "Jane Doe", "Senior Manager", "Sales", 95000, "2019-05-15"),   # Promotion
    ("E003", "Bob Johnson", "Analyst", "Finance", 68000, "2021-03-10"),     # Salary increase
    ("E004", "Alice Brown", "Developer", "IT", 70000, "2023-06-01")         # New employee
]

updated_employees = spark.createDataFrame(updated_employee_data, employee_schema)
```

#### SCD Type 1 Implementation (Detailed)

```python
class SCDType1Manager:
    def __init__(self, target_table):
        self.target_table = target_table
    
    def initialize_dimension(self, initial_df):
        """Initialize the dimension table"""
        enhanced_df = initial_df.withColumn("last_updated", current_timestamp())
        
        enhanced_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(self.target_table)
        
        print(f"Initialized {self.target_table} with {enhanced_df.count()} records")
    
    def apply_updates(self, source_df, key_column):
        """Apply SCD Type 1 updates"""
        
        # Read current dimension
        current_dim = spark.table(self.target_table)
        
        # Add update timestamp to source
        source_with_timestamp = source_df.withColumn("last_updated", current_timestamp())
        
        # Identify changes (any non-key column difference)
        changes = source_with_timestamp.alias("source") \
            .join(current_dim.alias("target"), key_column, "inner") \
            .where(
                (col("source.name") != col("target.name")) |
                (col("source.position") != col("target.position")) |
                (col("source.department") != col("target.department")) |
                (col("source.salary") != col("target.salary"))
            ) \
            .select("source.*")
        
        # Identify new records
        new_records = source_with_timestamp.alias("source") \
            .join(current_dim.alias("target"), key_column, "left_anti")
        
        # Create Delta table reference
        delta_table = DeltaTable.forName(spark, self.target_table)
        
        # Apply changes
        if changes.count() > 0:
            delta_table.alias("target") \
                .merge(changes.alias("source"), f"target.{key_column} = source.{key_column}") \
                .whenMatchedUpdateAll() \
                .execute()
            
            print(f"Updated {changes.count()} existing records")
            
            # Show what changed
            print("Changes Applied:")
            changes.select(key_column, "name", "position", "department", "salary").show()
        
        # Insert new records
        if new_records.count() > 0:
            new_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.target_table)
            
            print(f"Inserted {new_records.count()} new records")
        
        return {
            "updated_records": changes.count(),
            "new_records": new_records.count()
        }
    
    def get_current_state(self):
        """Get current state of all employees"""
        return spark.table(self.target_table)

# Usage
scd1_manager = SCDType1Manager("dim_employee_scd1")
scd1_manager.initialize_dimension(initial_employees)
scd1_results = scd1_manager.apply_updates(updated_employees, "employee_id")

print("SCD Type 1 Current State:")
scd1_manager.get_current_state().show()
```

#### SCD Type 2 Implementation (Detailed)

```python
class SCDType2Manager:
    def __init__(self, target_table):
        self.target_table = target_table
    
    def initialize_dimension(self, initial_df, effective_date="2020-01-01"):
        """Initialize the SCD Type 2 dimension table"""
        
        enhanced_df = initial_df \
            .withColumn("effective_date", lit(effective_date).cast(DateType())) \
            .withColumn("end_date", lit(None).cast(DateType())) \
            .withColumn("is_current", lit(True)) \
            .withColumn("version", lit(1)) \
            .withColumn("record_hash", 
                       sha2(concat_ws("|", col("name"), col("position"), 
                                    col("department"), col("salary")), 256)) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("last_updated", current_timestamp())
        
        enhanced_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(self.target_table)
        
        print(f"Initialized {self.target_table} with {enhanced_df.count()} records")
    
    def apply_updates(self, source_df, key_column, effective_date="2023-06-01"):
        """Apply SCD Type 2 updates"""
        
        # Add hash to source data
        source_with_hash = source_df \
            .withColumn("record_hash", 
                       sha2(concat_ws("|", col("name"), col("position"), 
                                    col("department"), col("salary")), 256))
        
        # Get current active records
        current_active = spark.table(self.target_table).filter(col("is_current") == True)
        
        # Identify changes
        changes = source_with_hash.alias("source") \
            .join(current_active.alias("target"), key_column, "inner") \
            .where(col("source.record_hash") != col("target.record_hash")) \
            .select("source.*", col("target.version").alias("current_version"))
        
        # Identify new records
        new_records = source_with_hash.alias("source") \
            .join(current_active.alias("target"), key_column, "left_anti")
        
        delta_table = DeltaTable.forName(spark, self.target_table)
        
        # Process changes
        if changes.count() > 0:
            # Close existing records
            delta_table.alias("target") \
                .merge(changes.alias("source"), 
                      f"target.{key_column} = source.{key_column} AND target.is_current = true") \
                .whenMatchedUpdate(set = {
                    "end_date": lit(effective_date),
                    "is_current": lit(False),
                    "last_updated": current_timestamp()
                }) \
                .execute()
            
            # Insert new versions
            new_versions = changes \
                .withColumn("effective_date", lit(effective_date).cast(DateType())) \
                .withColumn("end_date", lit(None).cast(DateType())) \
                .withColumn("is_current", lit(True)) \
                .withColumn("version", col("current_version") + 1) \
                .withColumn("created_timestamp", current_timestamp()) \
                .withColumn("last_updated", current_timestamp()) \
                .drop("current_version")
            
            new_versions.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.target_table)
            
            print(f"Processed {changes.count()} changed records")
            
            # Show what changed
            print("Historical Changes:")
            self.show_employee_history(changes.select(key_column).collect())
        
        # Insert completely new records
        if new_records.count() > 0:
            new_employee_records = new_records \
                .withColumn("effective_date", lit(effective_date).cast(DateType())) \
                .withColumn("end_date", lit(None).cast(DateType())) \
                .withColumn("is_current", lit(True)) \
                .withColumn("version", lit(1)) \
                .withColumn("created_timestamp", current_timestamp()) \
                .withColumn("last_updated", current_timestamp())
            
            new_employee_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.target_table)
            
            print(f"Inserted {new_records.count()} new employee records")
        
        return {
            "changed_records": changes.count(),
            "new_records": new_records.count()
        }
    
    def show_employee_history(self, employee_list=None):
        """Show complete history for specified employees or all"""
        
        query = f"""
        SELECT 
            employee_id,
            name,
            position,
            department,
            salary,
            effective_date,
            end_date,
            is_current,
            version
        FROM {self.target_table}
        """
        
        if employee_list:
            emp_ids = [row.employee_id for row in employee_list]
            emp_filter = "'" + "','".join(emp_ids) + "'"
            query += f" WHERE employee_id IN ({emp_filter})"
        
        query += " ORDER BY employee_id, effective_date"
        
        return spark.sql(query)
    
    def get_current_state(self):
        """Get current state of all employees"""
        return spark.table(self.target_table).filter(col("is_current") == True)
    
    def get_point_in_time_view(self, as_of_date):
        """Get employee data as it was on a specific date"""
        
        return spark.sql(f"""
        SELECT 
            employee_id,
            name,
            position,
            department,
            salary,
            effective_date,
            end_date
        FROM {self.target_table}
        WHERE effective_date <= '{as_of_date}'
        AND (end_date IS NULL OR end_date > '{as_of_date}')
        """)

# Usage
scd2_manager = SCDType2Manager("dim_employee_scd2")
scd2_manager.initialize_dimension(initial_employees)
scd2_results = scd2_manager.apply_updates(updated_employees, "employee_id")

print("\nSCD Type 2 Current State:")
scd2_manager.get_current_state().show()

print("\nComplete Employee History:")
scd2_manager.show_employee_history().show(truncate=False)

print("\nPoint-in-Time View (as of 2022-01-01):")
scd2_manager.get_point_in_time_view("2022-01-01").show()
```

### Performance and Storage Analysis

```python
def compare_scd_performance():
    """Compare storage and query performance between SCD1 and SCD2"""
    
    # Storage comparison
    scd1_size = spark.sql("DESCRIBE DETAIL dim_employee_scd1").select("sizeInBytes").collect()[0][0]
    scd2_size = spark.sql("DESCRIBE DETAIL dim_employee_scd2").select("sizeInBytes").collect()[0][0]
    
    print("Storage Comparison:")
    print(f"SCD Type 1: {scd1_size} bytes")
    print(f"SCD Type 2: {scd2_size} bytes")
    print(f"SCD2 is {scd2_size/scd1_size:.1f}x larger")
    
    # Query performance comparison
    import time
    
    # Simple current state query
    start_time = time.time()
    scd1_current = spark.table("dim_employee_scd1").count()
    scd1_time = time.time() - start_time
    
    start_time = time.time()
    scd2_current = spark.table("dim_employee_scd2").filter(col("is_current") == True).count()
    scd2_time = time.time() - start_time
    
    print(f"\nQuery Performance (Current State):")
    print(f"SCD Type 1: {scd1_current} records in {scd1_time:.4f} seconds")
    print(f"SCD Type 2: {scd2_current} records in {scd2_time:.4f} seconds")
    
    return {
        "storage_ratio": scd2_size/scd1_size,
        "performance_ratio": scd2_time/scd1_time
    }

# Run comparison
# performance_comparison = compare_scd_performance()
```

---

## 7. Parameters and Variables in ADF {#adf-parameters}

### Overview
Azure Data Factory (ADF) provides two main mechanisms for passing dynamic values: Parameters and Variables. Understanding their differences and use cases is crucial for building flexible pipelines.

### Parameters vs Variables

| Aspect | Parameters | Variables |
|--------|------------|-----------|
| **Scope** | Pipeline, Dataset, Linked Service | Pipeline only |
| **Mutability** | Read-only within scope | Can be modified during execution |
| **Use Case** | Configuration, External inputs | Dynamic calculations, Temporary storage |
| **Inheritance** | Can be passed between activities | Local to pipeline |

### Pipeline Parameters

#### Scenario: Dynamic Date-based ETL Pipeline

```json
{
    "name": "DynamicETLPipeline",
    "properties": {
        "parameters": {
            "ProcessDate": {
                "type": "String",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            },
            "SourcePath": {
                "type": "String",
                "defaultValue": "/data/raw"
            },
            "TargetTable": {
                "type": "String",
                "defaultValue": "processed_data"
            },
            "Environment": {
                "type": "String",
                "defaultValue": "dev"
            }
        },
        "variables": {
            "RecordCount": {
                "type": "Integer",
                "defaultValue": 0
            },
            "ProcessingStatus": {
                "type": "String",
                "defaultValue": "Started"
            },
            "ErrorMessage": {
                "type": "String",
                "defaultValue": ""
            }
        }
    }
}
```

### System Variables in ADF Pipeline

ADF provides several built-in system variables that provide runtime information:

#### Common System Variables

```json
{
    "name": "SystemVariablesExample",
    "activities": [
        {
            "name": "LogSystemInfo",
            "type": "WebActivity",
            "typeProperties": {
                "url": "https://logging-service.com/log",
                "method": "POST",
                "body": {
                    "pipelineName": "@pipeline().Pipeline",
                    "runId": "@pipeline().RunId",
                    "triggerTime": "@pipeline().TriggerTime",
                    "triggerName": "@pipeline().TriggerName",
                    "triggerType": "@pipeline().TriggerType",
                    "dataFactory": "@pipeline().DataFactory",
                    "currentUTC": "@utcnow()",
                    "groupId": "@pipeline().GroupId"
                }
            }
        }
    ]
}
```

---

## 8. Data Warehouse & ETL {#datawarehouse-etl}

### Data Warehouse Fundamentals

#### Architecture Components

Modern data warehouses follow a layered architecture approach:

1. **Bronze Layer (Raw)**: Ingested data as-is
2. **Silver Layer (Cleansed)**: Cleaned and validated data
3. **Gold Layer (Curated)**: Business-ready aggregated data

### ETL vs ELT Comparison

| Aspect | ETL (Extract, Transform, Load) | ELT (Extract, Load, Transform) |
|--------|-------------------------------|-------------------------------|
| **Processing Location** | External processing engine | Target system |
| **Data Transformation** | Before loading | After loading |
| **Storage Requirements** | Less raw data storage | More raw data storage |
| **Flexibility** | Less flexible | More flexible |
| **Processing Power** | Dedicated ETL tools | Leverage target system power |
| **Best For** | Structured data, Complex transformations | Big data, Cloud platforms |

---

## 9. Fact Tables and Dimension Tables {#fact-dimension-tables}

### Overview
Fact and Dimension tables form the foundation of dimensional modeling, following the star schema or snowflake schema design patterns.

### Fact Tables

**Characteristics:**
- Contain quantitative data (measures)
- High volume, frequently updated
- Foreign keys to dimension tables
- Additive, semi-additive, or non-additive measures

### Dimension Tables

**Characteristics:**
- Contain descriptive attributes
- Lower volume, less frequently updated
- Natural keys and surrogate keys
- Hierarchical relationships

---

## 10. Azure Synapse {#azure-synapse}

### Overview
Azure Synapse Analytics is a unified analytics service that combines big data and data warehousing capabilities.

### Key Components

1. **SQL Pools**: Dedicated and serverless compute
2. **Spark Pools**: Apache Spark clusters
3. **Data Integration**: Built-in ETL/ELT capabilities
4. **Analytics Runtime**: Machine learning and AI

### Implementation Example

```sql
-- Create dedicated SQL pool
CREATE TABLE FactSales (
    SalesKey BIGINT IDENTITY(1,1),
    DateKey INT,
    ProductKey INT,
    CustomerKey INT,
    SalesAmount DECIMAL(18,2),
    Quantity INT
)
WITH (
    DISTRIBUTION = HASH(ProductKey),
    CLUSTERED COLUMNSTORE INDEX
);

-- Create external table for data lake integration
CREATE EXTERNAL TABLE ExternalSales (
    SalesDate DATE,
    ProductId VARCHAR(50),
    SalesAmount DECIMAL(18,2)
)
WITH (
    LOCATION = '/sales/2023/',
    DATA_SOURCE = DataLakeSource,
    FILE_FORMAT = ParquetFormat
);
```

---

## 11. Streaming vs Batch Processing & Delta Tables {#streaming-batch}

### Batch Processing

**Characteristics:**
- Processes data in large chunks
- Higher latency, higher throughput
- Cost-effective for non-urgent processing
- Suitable for historical analysis

### Streaming Processing

**Characteristics:**
- Processes data in real-time
- Lower latency, continuous processing
- Higher cost for infrastructure
- Suitable for real-time analytics

### Delta Tables

Delta Lake provides ACID transactions, schema evolution, and time travel capabilities.

```python
# Batch processing with Delta
df = spark.read.format("delta").load("/path/to/delta-table")
processed_df = df.groupBy("category").sum("amount")
processed_df.write.format("delta").mode("overwrite").save("/path/to/output")

# Streaming with Delta
streaming_df = spark.readStream.format("delta").load("/path/to/streaming-source")
query = streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start("/path/to/delta-sink")
```

---

## 12. SCD2 Implementation using Databricks PySpark and ADF {#scd2-implementation}

### Complete SCD2 Pipeline

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import *

def implement_scd2_pipeline(source_df, target_table, business_key, effective_date):
    """
    Complete SCD2 implementation with Databricks and ADF integration
    """
    
    # Add hash for change detection
    source_with_hash = source_df.withColumn(
        "record_hash",
        sha2(concat_ws("|", *[col(c) for c in source_df.columns if c != business_key]), 256)
    )
    
    # Read current dimension
    if spark.catalog.tableExists(target_table):
        current_dim = spark.table(target_table)
        current_active = current_dim.filter(col("is_current") == True)
        
        # Identify changes
        changes = source_with_hash.alias("source") \
            .join(current_active.alias("target"), business_key, "inner") \
            .where(col("source.record_hash") != col("target.record_hash")) \
            .select("source.*", col("target.surrogate_key").alias("old_surrogate_key"))
        
        # Identify new records
        new_records = source_with_hash.alias("source") \
            .join(current_active.alias("target"), business_key, "left_anti")
        
        delta_table = DeltaTable.forName(spark, target_table)
        
        # Close changed records
        if changes.count() > 0:
            delta_table.alias("target") \
                .merge(changes.alias("source"), f"target.{business_key} = source.{business_key} AND target.is_current = true") \
                .whenMatchedUpdate(set={
                    "end_date": lit(effective_date),
                    "is_current": lit(False),
                    "updated_timestamp": current_timestamp()
                }) \
                .execute()
            
            # Insert new versions of changed records
            new_versions = changes \
                .withColumn("surrogate_key", expr("uuid()")) \
                .withColumn("effective_date", lit(effective_date)) \
                .withColumn("end_date", lit(None).cast("date")) \
                .withColumn("is_current", lit(True)) \
                .withColumn("created_timestamp", current_timestamp()) \
                .drop("old_surrogate_key")
            
            new_versions.write.format("delta").mode("append").saveAsTable(target_table)
        
        # Insert completely new records
        if new_records.count() > 0:
            new_customer_records = new_records \
                .withColumn("surrogate_key", expr("uuid()")) \
                .withColumn("effective_date", lit(effective_date)) \
                .withColumn("end_date", lit(None).cast("date")) \
                .withColumn("is_current", lit(True)) \
                .withColumn("created_timestamp", current_timestamp())
            
            new_customer_records.write.format("delta").mode("append").saveAsTable(target_table)
    
    else:
        # Initial load
        initial_load = source_with_hash \
            .withColumn("surrogate_key", expr("uuid()")) \
            .withColumn("effective_date", lit(effective_date)) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("created_timestamp", current_timestamp())
        
        initial_load.write.format("delta").mode("overwrite").saveAsTable(target_table)

# ADF Integration - Call from ADF using Databricks Activity
implement_scd2_pipeline(source_df, "dim_customer_scd2", "customer_id", "2023-12-01")
```

---

## 13. Calling Stored Procedures in ADF {#stored-procedures-adf}

### Stored Procedure Activity

```json
{
    "name": "ExecuteStoredProcedure",
    "type": "SqlServerStoredProcedure",
    "linkedServiceName": {
        "referenceName": "SqlServerLinkedService",
        "type": "LinkedServiceReference"
    },
    "typeProperties": {
        "storedProcedureName": "sp_ProcessSalesData",
        "storedProcedureParameters": {
            "ProcessDate": {
                "value": "@pipeline().parameters.ProcessDate",
                "type": "DateTime"
            },
            "BatchSize": {
                "value": "@pipeline().parameters.BatchSize",
                "type": "Int32"
            },
            "OutputPath": {
                "value": "@pipeline().parameters.OutputPath",
                "type": "String"
            }
        }
    }
}
```

---

## 14. Parallelism in Databricks {#databricks-parallelism}

### Optimizing Spark Jobs

```python
from pyspark.sql import SparkSession

# Configure Spark for optimal parallelism
spark = SparkSession.builder \
    .appName("OptimizedProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Parallel processing with optimal partitioning
def optimize_dataframe_processing(df, partition_column=None):
    """
    Optimize DataFrame for parallel processing
    """
    
    # Check current partitioning
    current_partitions = df.rdd.getNumPartitions()
    optimal_partitions = spark.sparkContext.defaultParallelism * 2
    
    if partition_column:
        # Repartition by column for better parallelism
        optimized_df = df.repartition(optimal_partitions, col(partition_column))
    else:
        # Repartition evenly
        optimized_df = df.repartition(optimal_partitions)
    
    return optimized_df

# Parallel aggregations
def parallel_aggregation_example(large_df):
    """
    Example of parallel aggregation with multiple grouping sets
    """
    
    # Use grouping sets for parallel aggregation
    result = large_df.cube("category", "region", "year") \
        .agg(
            sum("sales_amount").alias("total_sales"),
            count("transaction_id").alias("transaction_count"),
            avg("sales_amount").alias("avg_sales")
        )
    
    return result

# Usage
# optimized_df = optimize_dataframe_processing(large_df, "date_column")
# results = parallel_aggregation_example(optimized_df)
```

---

## 15. Compression Types for Parquet Files {#parquet-compression}

### Parquet Compression Options

| Compression | Speed | Ratio | CPU Usage | Use Case |
|-------------|-------|-------|-----------|----------|
| **Uncompressed** | Fastest | None | Minimal | Development/Testing |
| **Snappy** | Fast | Good | Low | Real-time processing |
| **Gzip** | Slow | Excellent | High | Archival storage |
| **LZ4** | Very Fast | Good | Very Low | High-throughput scenarios |
| **Zstd** | Medium | Very Good | Medium | Balanced performance |

### Implementation Examples

```python
# Configure compression for different use cases

# Real-time processing - Snappy
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
real_time_df.write.parquet("/path/to/realtime/data")

# Archival storage - Gzip
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
archive_df.write.parquet("/path/to/archive/data")

# High-throughput - LZ4
spark.conf.set("spark.sql.parquet.compression.codec", "lz4")
high_volume_df.write.parquet("/path/to/highvolume/data")

# Balanced approach - Zstd
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
balanced_df.write.parquet("/path/to/balanced/data")
```

---

## 16. Additional Columns in ADF Copy Activity {#adf-additional-columns}

### Adding Additional Columns

```json
{
    "name": "CopyWithAdditionalColumns",
    "type": "Copy",
    "typeProperties": {
        "source": {
            "type": "DelimitedTextSource"
        },
        "sink": {
            "type": "AzureSqlSink"
        },
        "translator": {
            "type": "TabularTranslator",
            "mappings": [
                {
                    "source": { "name": "CustomerID" },
                    "sink": { "name": "customer_id" }
                },
                {
                    "source": { "name": "CustomerName" },
                    "sink": { "name": "customer_name" }
                }
            ],
            "additionalColumns": [
                {
                    "name": "load_timestamp",
                    "value": "$$TIMESTAMP"
                },
                {
                    "name": "source_file",
                    "value": "$$FILENAME"
                },
                {
                    "name": "file_path",
                    "value": "$$FILEPATH"
                },
                {
                    "name": "pipeline_name",
                    "value": {
                        "value": "@pipeline().Pipeline",
                        "type": "Expression"
                    }
                },
                {
                    "name": "run_id",
                    "value": {
                        "value": "@pipeline().RunId",
                        "type": "Expression"
                    }
                },
                {
                    "name": "environment",
                    "value": {
                        "value": "@pipeline().parameters.Environment",
                        "type": "Expression"
                    }
                },
                {
                    "name": "static_column",
                    "value": "PROCESSED"
                }
            ]
        }
    }
}
```

---

## Conclusion

This comprehensive guide covers all the essential data engineering topics requested, with detailed implementations, scenario-based examples, and best practices. Each section provides practical code examples that can be directly implemented in real-world projects.

### Key Takeaways

1. **Databricks Workflows** provide robust orchestration capabilities
2. **Corrupt record handling** is crucial for data quality
3. **Compression choice** impacts both performance and storage costs
4. **SCD techniques** enable proper historical data management
5. **Join operations** have different use cases and performance characteristics
6. **ADF parameters and variables** enable flexible pipeline design
7. **Modern data warehouse architecture** supports both batch and streaming workloads
8. **Fact and dimension tables** form the foundation of analytical data models
9. **Azure Synapse** provides unified analytics capabilities
10. **Parallelism optimization** is key to Spark performance

### Best Practices Summary

- Always implement data quality checks
- Choose appropriate compression based on use case
- Design for scalability and maintainability
- Use proper error handling and monitoring
- Implement security and governance from the start
- Document your data lineage and transformations
- Test thoroughly before production deployment

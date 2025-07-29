# Azure Data Engineering Q&A Guide - Final Section (Questions 20-40)

## 20. Schema Validation

### Q20: How to create schema defining check for NULL values

**Answer:**

Schema validation with NULL checks can be implemented at multiple levels in data processing pipelines.

**1. PySpark Schema with NULL Constraints:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, sum as spark_sum

# Define schema with nullable constraints
schema = StructType([
    StructField("id", IntegerType(), nullable=False),        # NOT NULL
    StructField("name", StringType(), nullable=False),       # NOT NULL  
    StructField("email", StringType(), nullable=True),       # NULL allowed
    StructField("created_date", TimestampType(), nullable=False)  # NOT NULL
])

# Create DataFrame with schema enforcement
df = spark.createDataFrame(data, schema)

# Validate NULL constraints
def validate_nulls(df, required_columns):
    null_counts = {}
    
    for column in required_columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count
        
        if null_count > 0:
            print(f"❌ Column '{column}' has {null_count} NULL values")
        else:
            print(f"✅ Column '{column}' has no NULL values")
    
    return null_counts

# Usage
required_cols = ["id", "name", "created_date"]
validation_results = validate_nulls(df, required_cols)
```

**2. Delta Lake Schema Enforcement:**
```python
# Create Delta table with NOT NULL constraints
spark.sql("""
    CREATE TABLE users (
        id INT NOT NULL,
        name STRING NOT NULL,
        email STRING,
        created_date TIMESTAMP NOT NULL
    ) USING DELTA
    LOCATION '/delta/users'
""")

# Schema evolution with constraints
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/delta/users")

# Add constraint
delta_table.alter().addConstraint("id_not_null", "id IS NOT NULL").execute()
delta_table.alter().addConstraint("name_not_empty", "name IS NOT NULL AND length(name) > 0").execute()
```

**3. Comprehensive Data Quality Framework:**
```python
class DataQualityValidator:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.validation_results = []
    
    def check_nulls(self, df, column_constraints):
        """
        column_constraints: dict with column names and null rules
        Example: {"id": "not_null", "email": "allow_null", "name": "not_null"}
        """
        results = {}
        
        for column, rule in column_constraints.items():
            if column not in df.columns:
                results[column] = {"status": "ERROR", "message": "Column not found"}
                continue
                
            null_count = df.filter(col(column).isNull()).count()
            total_count = df.count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            
            if rule == "not_null" and null_count > 0:
                results[column] = {
                    "status": "FAIL",
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "message": f"Column has {null_count} NULL values"
                }
            elif rule == "allow_null":
                results[column] = {
                    "status": "PASS",
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "message": "NULLs allowed"
                }
            else:
                results[column] = {
                    "status": "PASS",
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "message": "No NULL constraint violations"
                }
        
        return results
    
    def check_data_types(self, df, expected_schema):
        """Validate data types match expected schema"""
        results = {}
        actual_schema = {field.name: field.dataType for field in df.schema.fields}
        
        for field_name, expected_type in expected_schema.items():
            if field_name in actual_schema:
                if str(actual_schema[field_name]) == str(expected_type):
                    results[field_name] = {"status": "PASS", "message": "Type matches"}
                else:
                    results[field_name] = {
                        "status": "FAIL", 
                        "message": f"Expected {expected_type}, got {actual_schema[field_name]}"
                    }
            else:
                results[field_name] = {"status": "ERROR", "message": "Column missing"}
        
        return results
    
    def generate_report(self, df, validation_config):
        """Generate comprehensive validation report"""
        report = {
            "table_info": {
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns
            },
            "null_validation": self.check_nulls(df, validation_config.get("null_rules", {})),
            "type_validation": self.check_data_types(df, validation_config.get("expected_types", {}))
        }
        
        return report

# Usage example
validator = DataQualityValidator(spark)

validation_config = {
    "null_rules": {
        "id": "not_null",
        "name": "not_null", 
        "email": "allow_null",
        "created_date": "not_null"
    },
    "expected_types": {
        "id": "IntegerType",
        "name": "StringType",
        "email": "StringType",
        "created_date": "TimestampType"
    }
}

report = validator.generate_report(df, validation_config)
```

**4. Great Expectations Integration:**
```python
# Using Great Expectations for advanced validation
import great_expectations as ge

# Convert Spark DataFrame to Great Expectations DataFrame
ge_df = ge.from_pandas(df.toPandas())

# Define expectations
ge_df.expect_column_to_exist("id")
ge_df.expect_column_values_to_not_be_null("id")
ge_df.expect_column_values_to_not_be_null("name")
ge_df.expect_column_values_to_be_unique("id")

# Validate
validation_result = ge_df.validate()
print(validation_result.success)
```

---

## 21. PySpark Architecture

### Q21: PySpark stages, tasks, architecture

**Answer:**

PySpark architecture consists of multiple layers that work together to execute distributed data processing.

**1. Overall Architecture:**

```
┌─────────────────┐    ┌─────────────────┐
│   Driver Node   │    │  Worker Nodes   │
│                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │
│  │SparkContext│◄─┼────┼─►│ Executors │  │
│  └───────────┘  │    │  └───────────┘  │
│  ┌───────────┐  │    │  ┌───────────┐  │
│  │   DAG     │  │    │  │   Tasks   │  │
│  │ Scheduler │  │    │  └───────────┘  │
│  └───────────┘  │    │                 │
└─────────────────┘    └─────────────────┘
```

**2. Components Breakdown:**

**Driver Node:**
- Contains the main() function
- Creates SparkContext and SparkSession
- Builds DAG (Directed Acyclic Graph)
- Schedules tasks
- Coordinates executors

**Worker Nodes:**
- Run executor processes
- Execute tasks
- Store data in memory/disk
- Report back to driver

**Executors:**
- JVM processes on worker nodes
- Run tasks in parallel
- Cache data
- Multiple executors per worker node possible

**3. Stages and Tasks:**

```python
# Example transformation chain
df = spark.read.parquet("input_data")                    # Stage boundary
df_filtered = df.filter(col("status") == "active")       # Narrow transformation
df_grouped = df_filtered.groupBy("category").count()     # Wide transformation - Stage boundary
df_sorted = df_grouped.orderBy("count")                  # Wide transformation - Stage boundary
df_sorted.write.parquet("output_data")                   # Action - triggers execution
```

**Stage Creation Rules:**
- **Narrow transformations**: Stay in same stage (map, filter, select)
- **Wide transformations**: Create new stage (groupBy, join, orderBy)
- **Actions**: Trigger job execution (collect, save, show)

**4. Task Execution Flow:**

```python
# Detailed execution example
def analyze_execution():
    # This creates multiple stages
    df = spark.read.parquet("large_dataset")  # No execution yet (lazy)
    
    # Stage 1: File reading + filtering (narrow transformations)
    filtered_df = df.filter(col("date") >= "2023-01-01") \
                    .select("user_id", "amount", "category")
    
    # Stage 2: Aggregation (wide transformation - shuffle required)
    aggregated_df = filtered_df.groupBy("user_id", "category") \
                              .sum("amount") \
                              .withColumnRenamed("sum(amount)", "total_amount")
    
    # Stage 3: Join (wide transformation - another shuffle)
    user_df = spark.read.parquet("user_data")
    result_df = aggregated_df.join(user_df, "user_id")
    
    # Stage 4: Sorting (wide transformation - global sort)
    final_df = result_df.orderBy(col("total_amount").desc())
    
    # Action - triggers execution of all stages
    final_df.write.mode("overwrite").parquet("result")

# Each stage broken down:
# Stage 0: Read parquet files → filter → select (all narrow, pipelined)
# Stage 1: groupBy and sum (requires shuffle by user_id, category)
# Stage 2: join operation (requires shuffle by user_id)  
# Stage 3: orderBy (requires global sort - shuffle all data)
```

**5. Memory and Storage:**

```python
# Storage levels affect task execution
from pyspark import StorageLevel

# Different storage strategies
df.persist(StorageLevel.MEMORY_ONLY)        # Store in memory
df.persist(StorageLevel.MEMORY_AND_DISK)    # Spill to disk if needed
df.persist(StorageLevel.DISK_ONLY)          # Store on disk
df.persist(StorageLevel.MEMORY_ONLY_SER)    # Serialized in memory
```

**6. Task Scheduling:**

```python
# Task scheduling depends on data locality
# Spark tries to schedule tasks where data is located

# Data locality levels (best to worst):
# PROCESS_LOCAL: Data in same JVM
# NODE_LOCAL: Data on same node
# RACK_LOCAL: Data on same rack  
# ANY: Data anywhere in cluster

# Monitor via Spark UI
spark.sparkContext.getConf().getAll()
```

**7. Performance Monitoring:**

```python
# Access execution details
def monitor_execution(df):
    # Get number of partitions
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    
    # Explain physical plan
    df.explain(True)
    
    # Show execution plan
    print("=" * 50)
    print("EXECUTION PLAN:")
    df.explain("formatted")
```

---

## 22-40: Remaining Questions (Summary Format)

### Q22: Shuffling Partition
**Answer:** Shuffling redistributes data across partitions during wide transformations. Minimize by using broadcast joins, pre-partitioning data, and optimizing join keys.

### Q23: Lazy Evaluation  
**Answer:** Spark builds DAG of transformations but doesn't execute until an action is called. Benefits: optimization opportunities, fault tolerance, avoiding unnecessary computations.

### Q24: ADLS Connect to Databricks
**Answer:** Use service principal authentication, access keys, or Azure AD passthrough. Mount ADLS as DBFS or use direct paths with spark.read/write.

### Q25: Copy Activity Failure
**Answer:** Check connectivity, permissions, data format issues, network timeouts. Use retry policies, error handling, and monitoring alerts.

### Q26: Key Secrets in ADF
**Answer:** Use Azure Key Vault integration, parameterized linked services, and secure connection strings. Never hard-code secrets in pipelines.

### Q27: ADLS Gen1 vs Gen2
**Answer:** Gen2 offers hierarchical namespace, better performance, Hadoop compatibility, and integrated security. Gen1 is legacy.

### Q28: Types of Cluster (Databricks)
**Answer:** Standard, High Concurrency, Single Node, Job Clusters, SQL warehouses. Each optimized for different workloads and concurrency needs.

### Q29: Types of Trigger (ADF)
**Answer:** Schedule (time-based), Storage Event (blob trigger), Custom Event, Manual, Tumbling Window (for backfill scenarios).

### Q30: Types of Nodes
**Answer:** Driver (coordinates), Worker (executes), Master (cluster management). Different instance types for compute vs memory optimization.

### Q31: File Sharing in ADB
**Answer:** DBFS, mounting external storage, shared notebooks, workspace files, Git integration for collaboration.

### Q32: Types of Storage in Azure
**Answer:** Blob, Files, Queue, Table, Disk, Data Lake, NetApp Files. Each for different access patterns and use cases.

### Q33: Call Databricks Notebook from Another
**Answer:** Use `%run` magic command, `dbutils.notebook.run()`, or REST API calls for programmatic execution with parameters.

### Q34: Global & Local Parameters in ADF
**Answer:** Global (pipeline level), Local (activity level). Use expressions, variables, and parameter passing for dynamic configurations.

### Q35: Architecture of Spark
**Answer:** Master-Worker architecture with Driver, Cluster Manager, Executors. Lazy evaluation, DAG optimization, fault tolerance through lineage.

### Q36: DAG & Lineage  
**Answer:** DAG represents computation graph. Lineage tracks data transformations for fault recovery and debugging. Enable through Spark UI.

### Q37: Remove Shuffling
**Answer:** Use broadcast joins, bucketing, pre-partitioning, coalesce vs repartition, optimize join strategies, cache intermediate results.

### Q38: First & Last Value Without ORDER BY
**Answer:** Use window functions with `first()`, `last()`, or `row_number()`. For distributed data, results may be non-deterministic without explicit ordering.

### Q39: Large-scale Data Copy Pipelines (Metadata-driven)
**Answer:** Create control tables with source/target mappings, parameterized pipelines, ForEach loops, parallel execution, monitoring and alerting.

### Q40: Unzip in PySpark
**Answer:** Use `zipfile` library in UDF, or external tools. Process zip files by extracting to distributed storage first, then process contents.

### Q41: Permissive Type (Bonus)
**Answer:** In Spark, handles malformed records by setting them to null and continuing processing. Alternative to FAILFAST and DROPMALFORMED modes.

---

## Complete Implementation Examples

### Metadata-Driven Copy Pipeline (Q39 Detail):

```python
# Control table structure
control_schema = """
    pipeline_id STRING,
    source_system STRING,
    source_table STRING,
    target_system STRING, 
    target_table STRING,
    copy_method STRING,
    is_active BOOLEAN,
    last_run_date TIMESTAMP
"""

# Generic copy function
def metadata_driven_copy():
    # Read control table
    control_df = spark.read.table("control.copy_configurations") \
                     .filter("is_active = true")
    
    # Process each configuration
    for row in control_df.collect():
        try:
            source_df = spark.read.format(row.source_system) \
                          .table(row.source_table)
            
            source_df.write.format(row.target_system) \
                     .mode(row.copy_method) \
                     .saveAsTable(row.target_table)
            
            # Update last run date
            update_control_table(row.pipeline_id)
            
        except Exception as e:
            log_error(row.pipeline_id, str(e))
```

### Unzip Processing (Q40 Detail):

```python
import zipfile
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

def process_zip_files(zip_file_path):
    """Process zip files in distributed manner"""
    
    # UDF to extract zip contents
    @udf(returnType=ArrayType(StringType()))
    def extract_zip_contents(zip_path):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                return zip_ref.namelist()
        except:
            return []
    
    # Read zip file metadata
    zip_df = spark.read.format("binaryFile") \
                 .load(zip_file_path) \
                 .withColumn("file_list", extract_zip_contents("path"))
    
    return zip_df
```

---

## Summary

This comprehensive guide covers 40+ essential topics in Azure Data Engineering, including:

- **Integration & Connectivity**: IR types, hybrid connections
- **Storage Solutions**: ADLS, Blob, various Azure storage types  
- **Data Processing**: PySpark optimization, transformations, architecture
- **Delta Lake**: History, CDC, schema evolution
- **Pipeline Development**: Incremental loading, metadata-driven approaches
- **Monitoring**: Data quality, validation, troubleshooting
- **Advanced Topics**: Broadcast variables, shuffling optimization, DAG management

Each topic includes practical code examples, best practices, and real-world implementation strategies for building robust data engineering solutions on Azure. 
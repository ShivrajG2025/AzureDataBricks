# Azure Data Engineering Q&A Guide - Part 3 (Questions 14-40)

## 14. Storage Comparison

### Q14: ADLS & BLOB difference

**Answer:**

Azure Data Lake Storage (ADLS) and Azure Blob Storage are both object storage services but optimized for different scenarios.

**Key Differences:**

| Aspect | Azure Blob Storage | Azure Data Lake Storage |
|--------|-------------------|-------------------------|
| **Namespace** | Flat namespace | Hierarchical namespace |
| **File System** | Object-based | POSIX-compliant directories |
| **Access Patterns** | Individual objects | Directory operations |
| **Analytics** | Basic | Optimized for big data |
| **Security** | Container/blob level | Directory/file level ACLs |
| **Protocols** | REST APIs | REST + HDFS APIs |
| **Pricing** | Per transaction | Optimized for analytics |
| **Use Cases** | Web apps, backups | Data lakes, analytics |

**Detailed Comparison:**

**1. Namespace Structure:**
```
// Blob Storage (Flat)
container/file1.txt
container/file2.txt
container/subfolder-file3.txt

// ADLS (Hierarchical)
filesystem/
  ├── folder1/
  │   ├── file1.txt
  │   └── subfolder/
  │       └── file2.txt
  └── folder2/
      └── file3.txt
```

**2. Security Models:**
```python
# Blob Storage - Container level
blob_service.set_container_acl('container', public_access='blob')

# ADLS - Directory/File level ACLs
file_system_client.set_access_control(
    path='folder1',
    permissions='rwxr--r--',
    acl='user::rwx,group::r--,other::r--'
)
```

**3. Performance Characteristics:**
- **Blob**: Optimized for individual object access
- **ADLS**: Optimized for directory operations and analytics

**4. Integration:**
- **Blob**: General Azure services
- **ADLS**: Deep integration with analytics services (HDInsight, Databricks, Synapse)

---

## 15. Spark Transformations

### Q15: Wide & Narrow Transformations

**Answer:**

Spark transformations are categorized based on their data shuffling requirements.

**Narrow Transformations:**
- Each input partition contributes to only one output partition
- No data shuffling across partitions
- Can be pipelined together
- Fast execution

**Examples of Narrow Transformations:**
```python
# map - 1:1 transformation
df.select(col("price") * 1.1)

# filter - reduces data
df.filter(col("age") > 18)

# flatMap - 1:many transformation
df.select(explode(split(col("text"), " ")))

# union - combines partitions
df1.union(df2)

# mapPartitions - operates on entire partition
df.mapPartitions(lambda partition: process_partition(partition))
```

**Wide Transformations:**
- Input partitions contribute to multiple output partitions
- Requires data shuffling across network
- Creates stage boundaries
- More expensive operations

**Examples of Wide Transformations:**
```python
# groupBy - requires shuffling by key
df.groupBy("category").count()

# join - may require shuffling both sides
df1.join(df2, "key")

# orderBy - requires global sorting
df.orderBy("timestamp")

# distinct - requires deduplication across partitions
df.distinct()

# repartition - redistributes data
df.repartition(10, "key")

# coalesce (when reducing partitions significantly)
df.coalesce(2)  # from many partitions to few
```

**Performance Implications:**

**1. Narrow Transformation Pipeline:**
```python
# These can be pipelined together efficiently
result = df.filter(col("status") == "active") \
           .select("id", "name", "price") \
           .withColumn("discounted_price", col("price") * 0.9)
```

**2. Wide Transformation Boundaries:**
```python
# Each wide transformation creates a stage boundary
stage1 = df.groupBy("category").sum("amount")  # Stage 1
stage2 = stage1.orderBy("sum(amount)")         # Stage 2
```

**Optimization Strategies:**
```python
# Minimize wide transformations
# Use broadcast joins for small tables
large_df.join(broadcast(small_df), "key")

# Partition data strategically
df.write.partitionBy("date").parquet("path")

# Use coalesce instead of repartition when reducing partitions
df.coalesce(5)  # Instead of repartition(5)
```

---

## 16. Broadcast Variables

### Q16: Broadcast Variables

**Answer:**

Broadcast variables allow efficient sharing of read-only data across all nodes in a Spark cluster without shipping the data with every task.

**Purpose:**
- Share large read-only datasets efficiently
- Reduce network traffic
- Improve performance for lookups and joins

**How Broadcast Variables Work:**
1. Data is serialized and sent to all nodes once
2. Stored in memory on each node
3. Tasks access local copy instead of downloading repeatedly

**Creating Broadcast Variables:**
```python
# Create broadcast variable
lookup_dict = {"A": 1, "B": 2, "C": 3}
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)

# Use in transformations
def map_values(row):
    return broadcast_lookup.value.get(row.category, 0)

df_mapped = df.rdd.map(map_values).toDF()
```

**DataFrame Broadcast Join:**
```python
from pyspark.sql.functions import broadcast

# Broadcast small table for join
large_df = spark.table("large_table")
small_df = spark.table("small_lookup")

# Broadcast join
result = large_df.join(broadcast(small_df), "key")
```

**Best Practices:**

**1. Size Considerations:**
```python
# Check broadcast variable size
import sys
data_size = sys.getsizeof(lookup_dict)
print(f"Data size: {data_size} bytes")

# Generally keep < 200MB for broadcast
if data_size < 200 * 1024 * 1024:  # 200MB
    broadcast_var = spark.sparkContext.broadcast(lookup_dict)
```

**2. Memory Management:**
```python
# Cleanup when done
broadcast_var.unpersist()
```

**3. Complex Data Structures:**
```python
# Broadcasting DataFrames as dictionaries
small_df_dict = small_df.rdd.collectAsMap()
broadcast_dict = spark.sparkContext.broadcast(small_df_dict)

def enrich_data(row):
    enrichment = broadcast_dict.value.get(row.key)
    return (row.id, row.value, enrichment)
```

**Use Cases:**
- Lookup tables
- Configuration data
- Machine learning models
- Reference data
- Currency conversion rates

---

## 17. Connectivity

### Q17: Connect ADF / Databricks to on-premise SQL Server

**Answer:**

Connecting Azure services to on-premises SQL Server requires proper network configuration and authentication.

**Method 1: Self-hosted Integration Runtime (ADF)**

**1. Install Self-hosted IR:**
```powershell
# Download and install Self-hosted Integration Runtime
# Register with Azure Data Factory
```

**2. Create Linked Service:**
```json
{
    "name": "OnPremSqlServerLinkedService",
    "type": "Microsoft.DataFactory/factories/linkedservices",
    "properties": {
        "type": "SqlServer",
        "connectVia": {
            "referenceName": "SelfHostedIR",
            "type": "IntegrationRuntimeReference"
        },
        "typeProperties": {
            "connectionString": "Server=myServerAddress;Database=myDB;Integrated Security=true;",
            "encryptedCredential": "encrypted_connection_string"
        }
    }
}
```

**3. ADF Pipeline Configuration:**
```json
{
    "name": "CopyFromOnPremSql",
    "properties": {
        "activities": [
            {
                "name": "CopyData",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "OnPremSqlDataset",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "AzureBlobDataset", 
                        "type": "DatasetReference"
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": "SELECT * FROM dbo.MyTable"
                    },
                    "sink": {
                        "type": "BlobSink"
                    }
                }
            }
        ]
    }
}
```

**Method 2: Databricks Connection**

**1. JDBC Connection:**
```python
# Databricks notebook
jdbc_url = "jdbc:sqlserver://your-server:1433;databaseName=your-db"

connection_properties = {
    "user": "username",
    "password": "password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read from SQL Server
df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.YourTable",
    properties=connection_properties
)

# Write to SQL Server
df.write.jdbc(
    url=jdbc_url,
    table="dbo.OutputTable",
    mode="append",
    properties=connection_properties
)
```

**2. Using Secret Scope:**
```python
# Store credentials in Databricks secret scope
username = dbutils.secrets.get(scope="sql-server", key="username")
password = dbutils.secrets.get(scope="sql-server", key="password")

connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
```

**Method 3: VPN/ExpressRoute Connection**

**1. Network Configuration:**
- Set up VPN or ExpressRoute
- Configure NSG rules
- Ensure SQL Server allows remote connections

**2. Databricks VNet Injection:**
```python
# Configure Databricks in your VNet
# Enable private endpoints
# Configure DNS resolution
```

**Security Best Practices:**
- Use Azure Key Vault for credentials
- Enable SSL/TLS encryption
- Use service accounts with minimal permissions
- Configure firewall rules
- Monitor connection logs

---

## 18. Self-hosted Integration Runtime

### Q18: IR - Self-hosted Setup

**Answer:**

Self-hosted Integration Runtime enables secure data movement between on-premises and cloud environments.

**Setup Process:**

**1. Prerequisites:**
- Windows Server 2012 R2 or later
- .NET Framework 4.7.2 or later
- PowerShell 5.1 or later
- Minimum 4 GB RAM, 8 GB recommended

**2. Installation Steps:**

```powershell
# Step 1: Download Self-hosted IR
# From Azure Data Factory portal -> Manage -> Integration Runtimes

# Step 2: Install the runtime
# Run installer as Administrator

# Step 3: Register with authentication key
# Copy key from ADF portal
Microsoft.DataFactory.IntegrationRuntime.SelfHosted.exe -RegisterNewNode [AuthKey]
```

**3. Configuration:**
```json
{
    "name": "MySelfHostedIR",
    "properties": {
        "type": "SelfHosted",
        "description": "Self-hosted IR for on-premises connectivity"
    }
}
```

**Advanced Configuration:**

**1. High Availability Setup:**
```powershell
# Install on multiple nodes
# Share the same authentication key
# Automatic failover capability

# Node 1 (Primary)
Microsoft.DataFactory.IntegrationRuntime.SelfHosted.exe -RegisterNewNode [AuthKey]

# Node 2 (Secondary) 
Microsoft.DataFactory.IntegrationRuntime.SelfHosted.exe -RegisterNewNode [AuthKey]
```

**2. Performance Tuning:**
```xml
<!-- In dmgcmd.exe.config -->
<configuration>
  <appSettings>
    <add key="MaxConcurrentJobs" value="8" />
    <add key="MaxConcurrentJobsPerNode" value="4" />
    <add key="SupportTimeZone" value="true" />
  </appSettings>
</configuration>
```

**3. Network Configuration:**
```json
{
    "proxySettings": {
        "httpProxy": "http://proxy-server:8080",
        "httpsProxy": "https://proxy-server:8080",
        "noProxyList": "localhost,127.0.0.1"
    }
}
```

**Monitoring and Troubleshooting:**

**1. Health Monitoring:**
```powershell
# Check IR status
Get-Service -Name "DIAHostService"

# View logs
Get-EventLog -LogName Application -Source "Microsoft.DataTransfer.*"
```

**2. Common Issues:**
- Port connectivity (443, 80, 9350-9354)
- Certificate validation
- Proxy configuration
- Firewall rules

**3. Diagnostics:**
```powershell
# Test connectivity
Test-NetConnection -ComputerName <ADF-endpoint> -Port 443

# Check service logs
Get-WinEvent -LogName "Microsoft-DataFactory-IntegrationRuntime/Operational"
```

---

## 19. Incremental Processing

### Q19: Incremental Process via ADF / Databricks - using watermark dataset / value

**Answer:**

Incremental processing loads only new or changed data since the last execution, using watermark values to track progress.

**ADF Incremental Loading:**

**1. Watermark Table Setup:**
```sql
-- Create watermark table
CREATE TABLE watermark_table (
    TableName NVARCHAR(255),
    WatermarkValue DATETIME2,
    LastModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- Initialize watermark
INSERT INTO watermark_table VALUES ('source_table', '1900-01-01', GETDATE());
```

**2. ADF Pipeline Configuration:**
```json
{
    "name": "IncrementalCopyPipeline",
    "properties": {
        "parameters": {
            "sourceTableName": {
                "type": "String",
                "defaultValue": "dbo.source_table"
            }
        },
        "activities": [
            {
                "name": "LookupOldWatermark",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": "SELECT WatermarkValue FROM watermark_table WHERE TableName = '@{pipeline().parameters.sourceTableName}'"
                    }
                }
            },
            {
                "name": "LookupNewWatermark", 
                "type": "Lookup",
                "dependsOn": [{"activity": "LookupOldWatermark", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": "SELECT MAX(LastModifiedDate) as NewWatermarkValue FROM @{pipeline().parameters.sourceTableName}"
                    }
                }
            },
            {
                "name": "IncrementalCopy",
                "type": "Copy", 
                "dependsOn": [{"activity": "LookupNewWatermark", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": "SELECT * FROM @{pipeline().parameters.sourceTableName} WHERE LastModifiedDate > '@{activity('LookupOldWatermark').output.firstRow.WatermarkValue}' AND LastModifiedDate <= '@{activity('LookupNewWatermark').output.firstRow.NewWatermarkValue}'"
                    }
                }
            },
            {
                "name": "UpdateWatermark",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [{"activity": "IncrementalCopy", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "storedProcedureName": "usp_write_watermark",
                    "storedProcedureParameters": {
                        "LastModifiedtime": "@{activity('LookupNewWatermark').output.firstRow.NewWatermarkValue}",
                        "TableName": "@{pipeline().parameters.sourceTableName}"
                    }
                }
            }
        ]
    }
}
```

**Databricks Incremental Processing:**

**1. Delta Lake Auto Loader:**
```python
# Auto Loader for incremental file processing
checkpoint_path = "/tmp/delta/checkpoint"
source_path = "/mnt/source/data"
target_path = "/mnt/delta/target"

# Stream processing with watermark
stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("checkpointLocation", checkpoint_path) \
    .load(source_path)

# Add watermark for late arriving data
watermarked_stream = stream.withWatermark("timestamp", "10 minutes")

# Write to Delta table
query = watermarked_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(target_path)
```

**2. Batch Incremental with Delta:**
```python
from delta.tables import DeltaTable
from datetime import datetime, timedelta

def incremental_load(source_table, target_path, watermark_column, last_watermark=None):
    # Get current watermark
    if last_watermark is None:
        last_watermark = get_last_watermark(target_path)
    
    # Read incremental data
    current_time = datetime.now()
    incremental_df = spark.sql(f"""
        SELECT * FROM {source_table} 
        WHERE {watermark_column} > '{last_watermark}' 
        AND {watermark_column} <= '{current_time}'
    """)
    
    if incremental_df.count() > 0:
        # Upsert to Delta table
        if DeltaTable.isDeltaTable(spark, target_path):
            delta_table = DeltaTable.forPath(spark, target_path)
            delta_table.alias("target").merge(
                incremental_df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Initial load
            incremental_df.write.format("delta").save(target_path)
        
        # Update watermark
        update_watermark(target_path, current_time)
    
    return incremental_df.count()

def get_last_watermark(target_path):
    try:
        # Read from watermark table or control table
        watermark_df = spark.read.table("control.watermarks") \
            .filter(f"table_path = '{target_path}'")
        
        if watermark_df.count() > 0:
            return watermark_df.collect()[0]['last_watermark']
        else:
            return datetime(1900, 1, 1)
    except:
        return datetime(1900, 1, 1)

def update_watermark(target_path, new_watermark):
    # Update watermark in control table
    watermark_update = [(target_path, new_watermark, datetime.now())]
    watermark_df = spark.createDataFrame(watermark_update, 
                                       ["table_path", "last_watermark", "updated_at"])
    
    watermark_df.write.mode("append").table("control.watermarks")
```

**3. Change Data Capture (CDC):**
```python
# Enable CDC on source table
spark.sql("""
    ALTER TABLE source_table 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read CDC data
cdf_df = spark.read.format("delta") \
    .option("readChangeDataFeed", "true") \
    .option("startingTimestamp", last_processed_timestamp) \
    .table("source_table")

# Process changes
inserts = cdf_df.filter("_change_type = 'insert'")
updates = cdf_df.filter("_change_type = 'update_postimage'")
deletes = cdf_df.filter("_change_type = 'delete'")
```

---

This covers questions 14-19. Would you like me to continue with the remaining questions (20-40) to complete the comprehensive guide? 
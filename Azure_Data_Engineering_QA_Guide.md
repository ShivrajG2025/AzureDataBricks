import com.itextpdf.text.*;
import com.itextpdf.text.pdf.*;
import java.io.FileOutputStream;

public class AzureDataEngineeringQAGuidePDF {

    public static void main(String[] args) {
        Document document = new Document();
        try {
            PdfWriter.getInstance(document, new FileOutputStream("Azure_Data_Engineering_QA_Guide.pdf"));
            document.open();

            // Title
            Font titleFont = new Font(Font.FontFamily.HELVETICA, 20, Font.BOLD);
            Paragraph title = new Paragraph("Azure Data Engineering - Comprehensive Q&A Guide", titleFont);
            title.setAlignment(Element.ALIGN_CENTER);
            document.add(title);
            document.add(Chunk.NEWLINE);

            // Table of Contents
            Font sectionFont = new Font(Font.FontFamily.HELVETICA, 14, Font.BOLD);
            document.add(new Paragraph("Table of Contents", sectionFont));
            List toc = new List(List.ORDERED);
            toc.add(new ListItem("Integration Runtime"));
            toc.add(new ListItem("Tables and Storage"));
            toc.add(new ListItem("Azure Data Lake Storage"));
            toc.add(new ListItem("PySpark Optimization"));
            toc.add(new ListItem("Cluster Management"));
            toc.add(new ListItem("Delta Lake"));
            toc.add(new ListItem("Spark Architecture"));
            toc.add(new ListItem("Data Processing"));
            toc.add(new ListItem("Azure Services Integration"));
            toc.add(new ListItem("Advanced Topics"));
            document.add(toc);
            document.add(Chunk.NEWLINE);

            // Section 1: Integration Runtime
            document.add(new Paragraph("1. Integration Runtime", sectionFont));
            document.add(new Paragraph("Q1: Integration Runtime - Types", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer: Integration Runtime (IR) is the compute infrastructure used by Azure Data Factory to provide data integration capabilities across different network environments."));
            document.add(new Paragraph("Types of Integration Runtime:"));
            com.itextpdf.text.List irList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            irList.add(new ListItem("Azure Integration Runtime (Azure IR)\n   - Default compute infrastructure in Azure\n   - Supports copy activities between cloud data stores\n   - Supports data flow activities\n   - Provides dispatch activities like lookup, get metadata\n   - Automatically managed by Microsoft\n   - No setup required"));
            irList.add(new ListItem("Self-hosted Integration Runtime (Self-hosted IR)\n   - Customer-managed compute infrastructure\n   - Installed on on-premises machines or VMs\n   - Enables hybrid data integration scenarios\n   - Supports copying data between cloud and on-premises\n   - Provides secure data movement\n   - Requires manual installation and configuration"));
            irList.add(new ListItem("Azure-SSIS Integration Runtime (Azure-SSIS IR)\n   - Managed cluster of Azure VMs\n   - Dedicated to running SSIS packages\n   - Lift-and-shift SSIS workloads to Azure\n   - Supports both Azure and on-premises data sources\n   - Fully managed service"));
            document.add(irList);
            document.add(new Paragraph("Use Cases:\n- Azure IR: Cloud-to-cloud data movement\n- Self-hosted IR: Hybrid scenarios, on-premises connectivity\n- Azure-SSIS IR: SSIS package execution in Azure"));
            document.add(Chunk.NEWLINE);

            // Section 2: Tables and Storage
            document.add(new Paragraph("2. Tables and Storage", sectionFont));
            document.add(new Paragraph("Q2: Physical & External Table", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer:"));
            document.add(new Paragraph("Physical Tables:"));
            com.itextpdf.text.List ptList = new com.itextpdf.text.List(com.itextpdf.text.List.UNORDERED);
            ptList.add(new ListItem("Data is physically stored in the database/storage system"));
            ptList.add(new ListItem("Tables own the data and storage"));
            ptList.add(new ListItem("Data is managed by the database engine"));
            ptList.add(new ListItem("Schema and data are tightly coupled"));
            ptList.add(new ListItem("Examples: Regular SQL Server tables, managed Delta tables"));
            document.add(ptList);

            document.add(new Paragraph("External Tables:"));
            com.itextpdf.text.List etList = new com.itextpdf.text.List(com.itextpdf.text.List.UNORDERED);
            etList.add(new ListItem("Metadata definition pointing to external data"));
            etList.add(new ListItem("Data is stored outside the database/warehouse"));
            etList.add(new ListItem("Table definition is separate from data storage"));
            etList.add(new ListItem("Schema is defined but data remains in original location"));
            etList.add(new ListItem("Examples: Synapse external tables, Databricks external tables"));
            document.add(etList);

            // Key Differences Table
            PdfPTable table1 = new PdfPTable(3);
            table1.setWidthPercentage(100);
            table1.addCell("Aspect");
            table1.addCell("Physical Table");
            table1.addCell("External Table");
            table1.addCell("Data Location");
            table1.addCell("Inside database");
            table1.addCell("External storage");
            table1.addCell("Data Ownership");
            table1.addCell("Database manages");
            table1.addCell("External system");
            table1.addCell("Performance");
            table1.addCell("Optimized");
            table1.addCell("Depends on external source");
            table1.addCell("Data Movement");
            table1.addCell("Required");
            table1.addCell("Not required");
            table1.addCell("Storage Cost");
            table1.addCell("Database storage");
            table1.addCell("External storage");
            table1.addCell("Schema Evolution");
            table1.addCell("Database controlled");
            table1.addCell("External source dependent");
            document.add(table1);

            document.add(new Paragraph("Examples:"));
            document.add(new Paragraph("-- Physical Table (Synapse)\nCREATE TABLE PhysicalTable (\n    ID INT,\n    Name VARCHAR(100)\n);\n\n-- External Table (Synapse)\nCREATE EXTERNAL TABLE ExternalTable (\n    ID INT,\n    Name VARCHAR(100)\n)\nWITH (\n    LOCATION = '/data/table/',\n    DATA_SOURCE = ExternalDataSource,\n    FILE_FORMAT = ParquetFormat\n);", FontFactory.getFont(FontFactory.COURIER, 10)));
            document.add(Chunk.NEWLINE);

            // Section 3: Azure Data Lake Storage
            document.add(new Paragraph("3. Azure Data Lake Storage", sectionFont));
            document.add(new Paragraph("Q3: ADLS (Azure Data Lake Storage)", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer: Azure Data Lake Storage (ADLS) is a scalable data storage service optimized for big data analytics workloads."));
            document.add(new Paragraph("Key Features:"));
            com.itextpdf.text.List adlsList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            adlsList.add(new ListItem("Hierarchical Namespace\n   - Directory and file-based organization\n   - POSIX-compliant file system semantics\n   - Efficient operations on directories"));
            adlsList.add(new ListItem("Security\n   - Role-based access control (RBAC)\n   - Access Control Lists (ACLs)\n   - Integration with Azure Active Directory\n   - Encryption at rest and in transit"));
            adlsList.add(new ListItem("Performance\n   - Optimized for analytics workloads\n   - High throughput and low latency\n   - Parallel processing capabilities"));
            adlsList.add(new ListItem("Integration\n   - Native integration with Azure services\n   - Hadoop-compatible (HDFS)\n   - Support for various analytics frameworks"));
            document.add(adlsList);

            document.add(new Paragraph("ADLS Gen2 Architecture:"));
            document.add(new Paragraph("Storage Account\n├── Container (File System)\n│   ├── Directory 1\n│   │   ├── file1.parquet\n│   │   └── file2.json\n│   └── Directory 2\n│       ├── Subdirectory\n│       └── file3.csv", FontFactory.getFont(FontFactory.COURIER, 10)));

            document.add(new Paragraph("Access Methods:\n- REST APIs\n- Azure Storage SDKs\n- HDFS drivers\n- Azure Storage Explorer\n- Various analytics tools (Databricks, Synapse, etc.)"));
            document.add(Chunk.NEWLINE);

            // Section 4: PySpark Optimization
            document.add(new Paragraph("4. PySpark Optimization", sectionFont));
            document.add(new Paragraph("Q4: Optimization Techniques - PySpark", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer:"));
            com.itextpdf.text.List pysparkList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            pysparkList.add(new ListItem("Data Serialization\nspark.conf.set(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\")"));
            pysparkList.add(new ListItem("Partitioning Strategies\ndf.write.partitionBy(\"year\", \"month\").parquet(\"path\")\ndf.coalesce(10).write.parquet(\"path\")"));
            pysparkList.add(new ListItem("Caching and Persistence\ndf.cache()\ndf.persist(StorageLevel.MEMORY_AND_DISK)\ndf.unpersist()"));
            pysparkList.add(new ListItem("Broadcast Variables\nbroadcast_dict = spark.sparkContext.broadcast(lookup_dict)"));
            pysparkList.add(new ListItem("Column Pruning and Predicate Pushdown\ndf.select(\"col1\", \"col2\").filter(df.col1 > 100)"));
            pysparkList.add(new ListItem("Data Format Optimization\ndf.write.format(\"delta\").save(\"path\")\ndf.write.format(\"parquet\").save(\"path\")"));
            pysparkList.add(new ListItem("Join Optimization\nfrom pyspark.sql.functions import broadcast\nresult = large_df.join(broadcast(small_df), \"key\")"));
            pysparkList.add(new ListItem("Resource Configuration\nspark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")\nspark.conf.set(\"spark.sql.adaptive.coalescePartitions.enabled\", \"true\")"));
            pysparkList.add(new ListItem("Bucketing\ndf.write.bucketBy(10, \"join_key\").saveAsTable(\"bucketed_table\")"));
            document.add(pysparkList);
            document.add(Chunk.NEWLINE);

            // Section 5: Cluster Management
            document.add(new Paragraph("5. Cluster Management", sectionFont));
            document.add(new Paragraph("Q5: Types of Modes of Cluster", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer:"));
            document.add(new Paragraph("Databricks Cluster Modes:"));
            com.itextpdf.text.List clusterList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            clusterList.add(new ListItem("Standard Cluster\n- General-purpose clusters\n- Support multiple languages (Python, Scala, R, SQL)\n- Can be shared by multiple users\n- Suitable for interactive analytics\n- Supports auto-scaling"));
            clusterList.add(new ListItem("High Concurrency Cluster\n- Optimized for concurrent users\n- Enhanced security and isolation\n- Table access control\n- Process isolation for different users\n- Supports SQL, Python, R (limited Scala)"));
            clusterList.add(new ListItem("Single Node Cluster\n- No worker nodes, only driver\n- Cost-effective for small workloads\n- Good for development and testing\n- Limited by single machine resources"));
            document.add(clusterList);

            document.add(new Paragraph("Cluster Access Modes:"));
            com.itextpdf.text.List accessList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            accessList.add(new ListItem("No Isolation Shared\n- Multiple users can share cluster\n- No process isolation\n- Users can see each other's data"));
            accessList.add(new ListItem("Shared\n- Process isolation between users\n- Enhanced security\n- Table access control\n- Multiple users can access safely"));
            accessList.add(new ListItem("Single User\n- Assigned to specific user\n- Maximum performance for single user\n- No sharing restrictions"));
            document.add(accessList);

            document.add(new Paragraph("Azure Synapse Spark Pool Modes:"));
            com.itextpdf.text.List synapseList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            synapseList.add(new ListItem("Auto-scale\n- Dynamically adjusts node count\n- Based on workload requirements\n- Cost optimization"));
            synapseList.add(new ListItem("Fixed Size\n- Predefined number of nodes\n- Consistent performance\n- Predictable costs"));
            document.add(synapseList);
            document.add(Chunk.NEWLINE);

            // Section 6: Data Types and Variables
            document.add(new Paragraph("6. Data Types and Variables", sectionFont));
            document.add(new Paragraph("Q6: var & nvar difference", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer:"));
            document.add(new Paragraph("In SQL Server context:"));
            document.add(new Paragraph("VARCHAR (Variable Character)\n- Stores non-Unicode character data\n- 1 byte per character\n- Maximum length: 8,000 characters\n- Uses default database collation\n- More storage efficient for English text"));
            document.add(new Paragraph("NVARCHAR (National Variable Character)\n- Stores Unicode character data\n- 2 bytes per character\n- Maximum length: 4,000 characters\n- Supports international characters\n- Required for multilingual applications"));

            // Comparison Table
            PdfPTable table2 = new PdfPTable(3);
            table2.setWidthPercentage(100);
            table2.addCell("Aspect");
            table2.addCell("VARCHAR");
            table2.addCell("NVARCHAR");
            table2.addCell("Character Set");
            table2.addCell("Non-Unicode");
            table2.addCell("Unicode");
            table2.addCell("Storage");
            table2.addCell("1 byte/char");
            table2.addCell("2 bytes/char");
            table2.addCell("Max Length");
            table2.addCell("8,000 chars");
            table2.addCell("4,000 chars");
            table2.addCell("International Support");
            table2.addCell("Limited");
            table2.addCell("Full");
            table2.addCell("Storage Efficiency");
            table2.addCell("Higher");
            table2.addCell("Lower");
            table2.addCell("Use Case");
            table2.addCell("English only");
            table2.addCell("Multilingual");
            document.add(table2);

            document.add(new Paragraph("Examples:"));
            document.add(new Paragraph("-- VARCHAR example\nDECLARE @name VARCHAR(50) = 'John Doe';\n\n-- NVARCHAR example\nDECLARE @international_name NVARCHAR(50) = N'José García';", FontFactory.getFont(FontFactory.COURIER, 10)));
            document.add(new Paragraph("In Programming contexts (like JavaScript):\n- var: Function-scoped, can be redeclared, hoisted\n- let/const: Block-scoped, cannot be redeclared"));
            document.add(Chunk.NEWLINE);

            // Section 7: Job Cluster
            document.add(new Paragraph("7. Job Cluster", sectionFont));
            document.add(new Paragraph("Q7: Job Cluster", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer: A Job Cluster is a cluster that is created specifically to run a particular job and is terminated when that job completes."));
            document.add(new Paragraph("Key Characteristics:"));
            com.itextpdf.text.List jobList = new com.itextpdf.text.List(com.itextpdf.text.List.ORDERED);
            jobList.add(new ListItem("Lifecycle Management\n- Created when job starts\n- Terminated when job completes\n- Ephemeral and dedicated"));
            jobList.add(new ListItem("Cost Optimization\n- Pay only for job execution time\n- No idle cluster costs\n- Automatic resource cleanup"));
            jobList.add(new ListItem("Isolation\n- Dedicated resources for specific job\n- No resource contention\n- Enhanced security"));
            jobList.add(new ListItem("Configuration\n- Job-specific cluster sizing\n- Optimized for particular workload\n- Custom libraries and configurations"));
            document.add(jobList);

            // Job Cluster vs Interactive Cluster Table
            PdfPTable table3 = new PdfPTable(3);
            table3.setWidthPercentage(100);
            table3.addCell("Aspect");
            table3.addCell("Job Cluster");
            table3.addCell("Interactive Cluster");
            table3.addCell("Lifecycle");
            table3.addCell("Job duration");
            table3.addCell("Long-running");
            table3.addCell("Cost");
            table3.addCell("Job execution only");
            table3.addCell("Continuous");
            table3.addCell("Sharing");
            table3.addCell("Single job");
            table3.addCell("Multiple users");
            table3.addCell("Use Case");
            table3.addCell("Production jobs");
            table3.addCell("Development/Analysis");
            table3.addCell("Startup Time");
            table3.addCell("Cluster creation overhead");
            table3.addCell("Immediate");
            document.add(table3);

            document.add(new Paragraph("Example Configuration (Databricks):"));
            document.add(new Paragraph("{\n  \"new_cluster\": {\n    \"spark_version\": \"11.3.x-scala2.12\",\n    \"node_type_id\": \"Standard_DS3_v2\",\n    \"num_workers\": 2,\n    \"cluster_log_conf\": {\n      \"dbfs\": {\n        \"destination\": \"dbfs:/cluster-logs\"\n      }\n    }\n  },\n  \"notebook_task\": {\n    \"notebook_path\": \"/Users/user@example.com/my-notebook\"\n  }\n}", FontFactory.getFont(FontFactory.COURIER, 10)));
            document.add(new Paragraph("Best Practices:\n- Use for production ETL jobs\n- Configure appropriate cluster size\n- Enable logging for troubleshooting\n- Use spot instances for cost optimization"));
            document.add(Chunk.NEWLINE);

            // Section 8: Delta Lake
            document.add(new Paragraph("8. Delta Lake", sectionFont));
            document.add(new Paragraph("Q8: Delta Table -- delta_df.history()", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
            document.add(new Paragraph("Answer: The history() method in Delta Lake provides access to the transaction log and version history of a Delta table."));
            document.add(new Paragraph("Purpose:\n- Track all changes made to the table\n- Enable time travel queries\n- Audit data modifications\n- Debug data pipeline issues"));
            document.add(new Paragraph("Syntax:\n# Get full history\nhistory_df = delta_table.history()\n# Get limited history\nhistory_df = delta_table.history(limit=10)\n# Display history\ndisplay(history_df)", FontFactory.getFont(FontFactory.COURIER, 10)));

            // History Information Table
            PdfPTable table4 = new PdfPTable(2);
            table4.setWidthPercentage(100);
            table4.addCell("Column");
            table4.addCell("Description");
            table4.addCell("version");
            table4.addCell("Table version number");
            table4.addCell("timestamp");
            table4.addCell("When operation occurred");
            table4.addCell("userId");
            table4.addCell("User who performed operation");
            table4.addCell("userName");
            table4.addCell("User name");
            table4.addCell("operation");
            table4.addCell("Type of operation (WRITE, DELETE, etc.)");
            table4.addCell("operationParameters");
            table4.addCell("Parameters used in operation");
            table4.addCell("job");
            table4.addCell("Job information");
            table4.addCell("notebook");
            table4.addCell("Notebook information");
            table4.addCell("clusterId");
            table4.addCell("Cluster that performed operation");
            table4.addCell("readVersion");
            table4.addCell("Version read during operation");
            table4.addCell("isolationLevel");
            table4.addCell("Transaction isolation level");
            document.add(table4);

            document.add(new Paragraph("Example Usage:\nfrom delta.tables import DeltaTable\n# Create Delta table reference\ndelta_table = DeltaTable.forPath(spark, \"/path/to/delta/table\")\n# Get history\nhistory = delta_table.history()\n# Show recent operations\nhistory.select(\"version\", \"timestamp\", \"operation\", \"operationParameters\").show()\n# Time travel using version\ndf_v1 = spark.read.format(\"delta\").option(\"versionAsOf\", 1).load(\"/path/to/delta/table\")\n# Time travel using timestamp\ndf_yesterday = spark.read.format(\"delta\").option(\"timestampAsOf\", \"2023-01-01\").load(\"/path/to/delta/table\")", FontFactory.getFont(FontFactory.COURIER, 10)));

            document.add(new Paragraph("Common Operations in History:\n- WRITE: Data insertion/overwrite\n- DELETE: Row deletion\n- UPDATE: Row updates\n- MERGE: Merge operations\n- OPTIMIZE: File compaction\n- VACUUM: File cleanup"));
            document.add(Chunk.NEWLINE);

            document.add(new Paragraph("This is the first section of the comprehensive guide. Would you like me to continue with the remaining questions (9-40) in the next sections?"));

            document.close();
            System.out.println("PDF created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
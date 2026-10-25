# Engine Limitations

**[About](index.md)** | **[Azure Guide](azure_production_guide.md)** | **[Production Guide](production_adoption_guide.md)** | **[Limitations](limitations.md)** | **[Pros & Cons](pros_and_cons.md)** | **[Examples](examples.md)** | **[GitHub Repository](https://github.com/anilkulkarni87/databricks-lakehouse-boilerplate)**

---

To ensure complete engineering transparency, this document details the actual boundaries, design constraints, and current limitations of the **Lakehouse Engine** based strictly on its active codebase.

---

## 1. Pipeline Topology Constraints

### Single-Source Ingestion per Entity
* **Current Behavior**: The pipeline definition (`EntityPipelineConfig.source`) ingests from a single source Delta table or stream (`source.catalog.schema.table`).
* **Limitation**: Arbitrary multi-stream joins (e.g. streaming `orders` joined to streaming `payments` with watermarking) before landing in the Gold table cannot be declared inside a single YAML file.
* **Workaround**: Perform multi-source joins or complex event correlation upstream in a Silver landing view or staging table, and point Lakehouse Engine to that unified landing stream.

### Single Gold Target per Entity
* **Current Behavior**: Each YAML configuration manages one curated Gold target table (`gold.table`) plus optional 1-to-1 PII split tables (`pii_splits`).
* **Limitation**: A single YAML cannot fan out one stream into multiple distinct Gold star-schema fact and dimension tables simultaneously.

---

## 2. Storage & Engine Dependencies

### Delta Lake Exclusivity
* **Current Behavior**: Ingestion, upserts, and time-travel operations utilize the official Delta Lake Python API (`from delta.tables import DeltaTable`).
* **Limitation**: Target tables must be in **Delta** format. The engine does not support writing to Apache Iceberg or Apache Hudi targets.

### Databricks / Spark Runtime Requirement
* **Current Behavior**: The engine requires an active `SparkSession` with Delta extensions enabled (`io.delta.sql.DeltaSparkSessionExtension`).
* **Limitation**: It cannot execute on lightweight SQL engines (DuckDB, ClickHouse, SQLite) or purely serverless Python runtimes without PySpark.

### Modern Unity Catalog Volume Dependency
* **Current Behavior**: Streaming checkpoints must use Unity Catalog Volumes (`/Volumes/{catalog}/{schema}/{volume}/checkpoints/...`).
* **Limitation**: Legacy workspaces relying on the public DBFS root (`/tmp/...` or `dbfs:/tmp/...`) will fail with `DBFS_DISABLED` (`SQLSTATE: 56038`) on modern Databricks runtimes (DBR 14.x/15.x+).

---

## 3. Data Quality & Expectation Boundaries

### SQL String Expressions Only
* **Current Behavior**: Expectations are evaluated using Spark SQL filter expressions (`expr: "mrr_amount >= 0.0"`).
* **Limitation**: Custom, stateful Python functions (e.g. calling an external fraud detection REST API or complex machine learning model scoring) cannot be embedded directly in the expectation YAML.
* **Scope of Circuit Breakers**: Actions are strictly limited to `fail_batch` (halts the micro-batch), `quarantine` (routes the row to the DLQ table), and `warn` (logs a warning to the audit table).

---

## 4. Autonomous DLQ Triage Limits

### In-Memory Record Inspection
* **Current Behavior**: The DLQ triage module (`lakehouse_engine.core.triage`) pulls quarantined records into the driver memory (default limit: 500 rows) to perform clustering, AST analysis, and schema drift discovery.
* **Limitation**: Triage is designed for automated diagnostic runs on small-to-medium sample batches of failed rows. It is not designed to perform distributed full-table analytics over millions of historical DLQ records in a single triage invocation.

### Supervised Remediations
* **Current Behavior**: The `--apply-fix` flag generates a unified diff and modifies the YAML configuration file on disk.
* **Limitation**: It requires human approval or an automated CI pull request to merge the patched YAML. It does not unilaterally alter production table schemas without configuration updates.

---

## 5. Schema Evolution Policy

### No Unsupervised Column Additions
* **Current Behavior**: If an upstream JSON event introduces new unknown fields, they are preserved in `_rescued_payload` (or inside a `VARIANT` column).
* **Limitation**: The engine does not automatically execute runtime `ALTER TABLE ADD COLUMN` DDLs during streaming execution, preventing silent schema inflation in production Gold tables.

# Databricks Free Tier (Community Edition) End-to-End Verification Guide

This guide is the complete, battle-tested blueprint for running, verifying, and demonstrating the entire **Lakehouse Engine** on **Databricks Community Edition (100% Free Tier)**.

It covers the complete lifecycle:
1. **First Streaming Run** (Liquid Clustering, PII Isolation, Native `VARIANT`)
2. **Unity Catalog Semantic Comments & Genie AI Adoption**
3. **Bad Record Injection & Dead-Letter Queue (DLQ) Quarantine**
4. **Autonomous Self-Healing DLQ Triage**
5. **Historical Batch Backfill**
6. **Operational Micro-Batch Telemetry Audit**

---

## ⚠️ Modern Free Tier Architecture Rules (Crucial Learnings)

| Challenge | Why it Happens | The Golden Rule |
| :--- | :--- | :--- |
| **`%sh pip` vs `%pip`** | `%sh pip` installs into the container OS, not into the Python environment used by Spark streaming workers (`foreach_batch_worker.py`), causing `ModuleNotFoundError: No module named 'lakehouse_engine'`. | **Always use `%pip install`** in notebook Cell 1. `%pip` installs into the PySpark kernel and automatically restarts the environment across all driver and streaming workers. |
| **Hive Metastore Disabled** | Databricks has deprecated and disabled Hive Metastore (`hive_metastore`) by default on all modern Community Edition workspaces. | **Use Unity Catalog `main`** (or dynamic catalog detection `SHOW CATALOGS`). Never use legacy 2-level namespaces like `default.tbl`. |
| **Public DBFS Root Disabled** | Legacy paths starting with `dbfs:/tmp/...` or `/tmp/...` are blocked (`DBFS_DISABLED`). | **Always use Unity Catalog Volumes** for streaming checkpoints: `/Volumes/{catalog}/{schema}/{volume}/checkpoints/...`. |
| **YAML String Formatting** | Placing Python's `f"..."` syntax directly inside a YAML block is treated as literal characters `f"`, causing path syntax errors. | **Keep YAML plain string literals** or construct dynamically using Python string formatting. |

---

## 🛠️ Step 0: Build the Wheel Locally

In your local terminal:
```bash
task build
# Generates: dist/lakehouse_engine-0.1.0-py3-none-any.whl
```

Upload this `.whl` file to your Databricks workspace:
* Navigate to **Catalog** $\rightarrow$ select your Catalog (`main` or custom) $\rightarrow$ **default** $\rightarrow$ create a **Volume** named `library` (or `checkpoints`).
* Upload `lakehouse_engine-0.1.0-py3-none-any.whl` into that Volume.
* Path: `/Volumes/<catalog>/default/library/lakehouse_engine-0.1.0-py3-none-any.whl`.

---

## 📓 The Complete Databricks Notebook Script

Create a new Python notebook on your Databricks cluster and paste the following sequential cells:

### Cell 1: Install Engine Wheel on Cluster & Streaming Workers
```python
# COMMAND ----------
# MAGIC %pip install /Volumes/lakeshouse_engine/default/library/lakehouse_engine-0.1.0-py3-none-any.whl --force-reinstall
```
*(Replace `/Volumes/...` with the exact path where you uploaded your `.whl`). When this cell finishes, Databricks will notify: `Python kernel restarted to load new libraries`.*

---

### Cell 2: Resolve Active Catalog & Setup Schemas and Volumes
```python
# COMMAND ----------
# 1. Dynamically detect active catalog (Modern Community Edition uses 'main' or your custom catalog)
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
active_catalog = "lakeshouse_engine" if "lakeshouse_engine" in catalogs else ("main" if "main" in catalogs else catalogs[0])
print(f"✅ Active Unity Catalog: '{active_catalog}'")

spark.sql(f"USE CATALOG {active_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.raw_landing")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.default")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.default.library")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.default.checkpoints")
```

---

### Cell 3: Setup Silver Landing Stream Table & Insert Baseline Clean Record
```python
# COMMAND ----------
# 1. Create Silver landing events table with nested structs and raw JSON string
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {active_catalog}.raw_landing.silver_subscription_events (
  subscriptionId STRING,
  plan STRUCT<tier: STRING, billingInterval: STRING, amountCents: BIGINT>,
  currency STRING,
  status STRING,
  isTrial BOOLEAN,
  autoRenew BOOLEAN,
  renewalCount INT,
  startedOn TIMESTAMP,
  updatedOn TIMESTAMP,
  subscriberId STRING,
  subscriber STRUCT<email: STRING, contactName: STRING, phone: STRING, taxIdentifier: STRING, country: STRING, postalCode: STRING>,
  raw_device_headers STRING
) USING DELTA
""")

# 2. Insert clean baseline record
spark.sql(f"""
INSERT INTO {active_catalog}.raw_landing.silver_subscription_events VALUES (
  'sub_1001',
  named_struct('tier', 'Pro', 'billingInterval', 'monthly', 'amountCents', 4900),
  'USD',
  'ACTIVE',
  false,
  true,
  3,
  current_timestamp(),
  current_timestamp(),
  'cust_901',
  named_struct('email', 'alex.miller@example.com', 'contactName', 'Alex Miller', 'phone', '+1-555-0123', 'taxIdentifier', 'US-9912', 'country', 'US', 'postalCode', '98101'),
  '{{\"user_agent\": \"Mozilla/5.0\", \"ip\": \"192.168.1.1\", \"dark_mode\": true}}'
)
""")
```

---

### Cell 4: Define Declarative Pipeline Configuration with Governance & VARIANT
```python
# COMMAND ----------
import yaml
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.streaming import run_pipeline

checkpoint_volume_path = f"/Volumes/{active_catalog}/default/checkpoints/subscriptions"

config_yaml = f"""
version: "1.0"
entity_name: "subscriptions"

source:
  catalog: "{active_catalog}"
  schema: "raw_landing"
  table: "silver_subscription_events"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "{checkpoint_volume_path}"
  audit_table: "{active_catalog}.default.pipeline_batch_audit"

gold:
  catalog: "{active_catalog}"
  schema: "default"
  table: "dim_subscriptions"
  table_comment: "Curated SaaS subscription dimension with contract metrics and pseudonymized identity"
  table_type: "dimension"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  prune_column: "started_at"

  # Zero Data Loss: Dead-Letter Queue & Rescued Data
  enable_dlq: true
  dlq_table: "{active_catalog}.default.dim_subscriptions_dlq"
  track_rescued_data: true

  # Declarative Data Quality Expectations (Circuit Breakers)
  expectations:
    - name: "valid_mrr"
      expr: "mrr_amount >= 0.0"
      action: "fail_batch"
      description: "Monthly recurring revenue cannot be negative"
    - name: "valid_status"
      expr: "status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED')"
      action: "quarantine"
      description: "Only official subscription statuses allowed"

  fields:
    - name: "subscription_id"
      type: "string"
      expr: "subscriptionId"
      is_primary_key: true
      description: "Unique surrogate key for subscription contract"
    - name: "plan_tier"
      type: "string"
      expr: "plan.tier"
      description: "Subscription pricing tier (Starter, Pro, Enterprise)"
    - name: "status"
      type: "string"
      expr: "status"
      description: "Subscription lifecycle status"
    - name: "mrr_amount"
      type: "double"
      expr: "cast(plan.amountCents as double) / 100.0"
      description: "Monthly Recurring Revenue normalized in USD"
    - name: "started_at"
      type: "timestamp"
      expr: "startedOn"
      description: "Contract start timestamp"
    - name: "subscriber_account_id"
      type: "string"
      expr: "xxhash64(cast(subscriberId as string))"
      description: "Pseudonymized subscriber foreign key linking to dim_subscriber_profiles_pii"

    # Native Binary VARIANT column (Hardware-accelerated by Photon)
    - name: "device_metadata"
      type: "variant"
      expr: "raw_device_headers"
      description: "Device and client headers stored in native binary VARIANT"

pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "{active_catalog}"
    target_schema: "default"
    target_table: "dim_subscriber_profiles_pii"
    table_comment: "Restricted subscriber identity and PII profile dimension"
    unique_keys: ["subscriber_account_id"]
    foreign_key_expr: "xxhash64(cast(subscriberId as string))"
    foreign_key_name: "subscriber_account_id"
    cluster_by: ["created_at"]
    fields:
      - name: "subscriber_email"
        type: "string"
        expr: "subscriber.email"
        description: "Direct PII customer email address"
      - name: "contact_name"
        type: "string"
        expr: "subscriber.contactName"
        description: "Full customer contact authority name"
      - name: "created_at"
        type: "timestamp"
        expr: "startedOn"
        description: "Profile creation timestamp"
"""

config = EntityPipelineConfig.model_validate(yaml.safe_load(config_yaml))
print(f"✅ Loaded configuration for entity: '{config.entity_name}'")
```

---

### Cell 5: Execute First Streaming Run & Inspect Clean Data
```python
# COMMAND ----------
# 1. Execute streaming micro-batch
run_pipeline(spark, config)

# 2. View Gold Clean Dimension
print("=== GOLD CLEAN DIMENSION ===")
display(spark.table(f"{active_catalog}.default.dim_subscriptions"))

# 3. Query native binary VARIANT column with Photon syntax
print("=== QUERYING VARIANT COLUMNS (No JSON string parsing overhead) ===")
display(spark.sql(f"""
  SELECT 
    subscription_id,
    plan_tier,
    mrr_amount,
    device_metadata:ip::string AS client_ip,
    device_metadata:dark_mode::boolean AS dark_mode_enabled
  FROM {active_catalog}.default.dim_subscriptions
"""))

# 4. View Segregated PII Dimension
print("=== SEGREGATED PII TABLE ===")
display(spark.table(f"{active_catalog}.default.dim_subscriber_profiles_pii"))

# 5. View Micro-Batch Operational Telemetry Audit Log
print("=== MICRO-BATCH OPERATIONAL TELEMETRY LOG ===")
display(spark.table(f"{active_catalog}.default.pipeline_batch_audit"))
```

---

### Cell 6: Inspect Unity Catalog & Genie Semantic Column Comments
```python
# COMMAND ----------
# Verify that all table and column comments were registered in Unity Catalog metadata
display(spark.sql(f"DESCRIBE EXTENDED {active_catalog}.default.dim_subscriptions"))
```
*(Notice that `comment` is populated for all fields: `subscription_id`, `plan_tier`, `mrr_amount`, etc., as well as the Table Comment).*

---

### Cell 7: Inject Schema Drift & Bad Records (Testing DLQ Quarantine)
```python
# COMMAND ----------
# Insert 2 malformed records:
# Record A: Violates 'valid_status' expectation ('PENDING_REVIEW' not in allowed set)
# Record B: Missing primary key (subscriptionId is NULL)
spark.sql(f"""
INSERT INTO {active_catalog}.raw_landing.silver_subscription_events VALUES (
  'sub_9999',
  named_struct('tier', 'Pro', 'billingInterval', 'monthly', 'amountCents', 4900),
  'USD',
  'PENDING_REVIEW',  -- Bad status!
  false,
  true,
  1,
  current_timestamp(),
  current_timestamp(),
  'cust_999',
  named_struct('email', 'drift@example.com', 'contactName', 'Drift User', 'phone', '+1-555-9999', 'taxIdentifier', 'US-99', 'country', 'US', 'postalCode', '10001'),
  '{{\"user_agent\": \"Mobile-App\", \"dark_mode\": false}}'
),
(
  NULL,              -- Missing primary key!
  named_struct('tier', 'Starter', 'billingInterval', 'monthly', 'amountCents', 1900),
  'USD',
  'ACTIVE',
  true,
  false,
  0,
  current_timestamp(),
  current_timestamp(),
  'cust_888',
  named_struct('email', 'nokey@example.com', 'contactName', 'No Key User', 'phone', '+1-555-8888', 'taxIdentifier', 'US-88', 'country', 'US', 'postalCode', '10002'),
  '{{\"user_agent\": \"Web-Browser\", \"dark_mode\": true}}'
)
""")

# Re-run streaming pipeline
run_pipeline(spark, config)

# View quarantined DLQ records
print("=== DEAD-LETTER QUEUE (DLQ) QUARANTINED ROWS ===")
display(spark.table(f"{active_catalog}.default.dim_subscriptions_dlq"))
```

---

### Cell 8: Autonomous Self-Healing DLQ Triage
```python
# COMMAND ----------
from lakehouse_engine.core.triage import analyze_quarantined_records

# 1. Fetch quarantined records directly from Delta DLQ table
dlq_rows = [row.asDict(recursive=True) for row in spark.table(f"{active_catalog}.default.dim_subscriptions_dlq").collect()]

# 2. Run Autonomous Triage
report = analyze_quarantined_records(config, dlq_rows)

# 3. Print the diagnostic RCA and recommended self-healing YAML patch!
print(report.format_console_report())
```

---

### Cell 9: Historical Batch Backfill (`run_backfill`)
```python
# COMMAND ----------
from lakehouse_engine.core.backfill import run_backfill

# Execute historical batch backfill without disrupting streaming checkpoint offsets
run_backfill(
    spark=spark,
    config=config,
    start_date="2026-01-01",
    end_date="2026-12-31",
    date_column="started_at",
    reset_checkpoint=False
)
```

---

### Cell 10: Databricks Genie AI Adoption & Prompt Generation
```python
# COMMAND ----------
from lakehouse_engine.core.genie import generate_genie_space_documentation

# 1. Auto-compile Genie Space System Prompt and Informational Constraints DDL
genie_doc = generate_genie_space_documentation([config])
print(genie_doc)
```

#### How to Connect Databricks Genie in Free Tier / Enterprise:
1. In Databricks, click **Genie** in the left sidebar $\rightarrow$ click **New Genie Space**.
2. Select your catalog tables: `{active_catalog}.default.dim_subscriptions` and `{active_catalog}.default.dim_subscriber_profiles_pii`.
3. In **Instructions**, copy and paste the markdown output from Cell 10.
4. Ask natural language questions:
   * *"What is our Total MRR for Pro tier subscribers?"*
   * *"Show me the distribution of active subscriptions by country."*
   * *"How many active trials do we have?"*

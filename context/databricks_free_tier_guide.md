# Running on Databricks Free Tier (Community Edition) & Local PySpark

This guide provides step-by-step instructions for running, testing, and demonstrating this framework on **Databricks Community Edition (100% free)** or **locally on your laptop**.

---

## 1. Modern Free Tier Architecture & Common Pitfalls

In modern Databricks Community Edition (Databricks Runtime 14.x / 15.x+):

| Challenge | Root Cause | Solution Implemented |
| :--- | :--- | :--- |
| **Hive Metastore Disabled** | Databricks has deprecated and disabled Hive Metastore (`hive_metastore`) by default on new Community Edition workspaces. | Use Unity Catalog's default catalog **`main`** (or dynamic catalog detection `SHOW CATALOGS`). Do not use `spark_catalog` or legacy 2-level Hive namespaces. |
| **Streaming Runner `ModuleNotFoundError`** | Databricks Connect / Serverless executes `foreachBatch` inside an isolated streaming runner worker process (`foreach_batch_worker.py`). If the package was only added to driver `sys.path`, workers cannot deserialize the streaming closure. | Install the `.whl` cluster-wide via **Cluster Libraries**, `%pip install /path/to/wheel.whl` in notebook Cell 1, or `%pip install -e` in Git Folders. |

---

## 2. Running on Databricks Community Edition in 3 Steps

### Step 1: Build the Wheel Locally
From your local terminal, build the lightweight wheel package:
```bash
task build
# Outputs: dist/lakehouse_engine-0.1.0-py3-none-any.whl
```

### Step 2: Install Package on Databricks Cluster
Choose **one** of the following installation methods:

#### Method A: Via Databricks Cluster Libraries (Recommended)
1. In your Databricks Community Edition workspace, click **Compute** in the left sidebar.
2. Click your running cluster $\rightarrow$ select the **Libraries** tab.
3. Click **Install New** $\rightarrow$ select **Upload** $\rightarrow$ choose **Python Whl**.
4. Drop `dist/lakehouse_engine-0.1.0-py3-none-any.whl` and click **Install**.
*(Once installed, all driver and streaming worker processes have `lakehouse_engine` installed).*

#### Method B: Via Databricks Git Folders (Repos)
1. In Databricks, navigate to **Workspace** $\rightarrow$ **Repos** (or **Git Folders**).
2. Click **Add Repo** $\rightarrow$ paste `https://github.com/anilkulkarni87/databricks-lakehouse-boilerplate.git`.
3. In your notebook, run Cell 1:
   ```python
   %pip install -e /Workspace/Repos/<your_user_email>/databricks-lakehouse-boilerplate
   ```

---

### Step 3: Run the Complete End-to-End Notebook
Create a new Python notebook on your cluster and paste the following self-contained script:

```python
# COMMAND ----------
# MAGIC %md
# MAGIC # Databricks Lakehouse Engine: End-to-End Streaming & Governance Demo

# COMMAND ----------
# 1. Resolve Active Unity Catalog (Modern Community Edition uses 'main')
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
active_catalog = "main" if "main" in catalogs else catalogs[0]
print(f"✅ Using active Databricks Catalog: '{active_catalog}'")

spark.sql(f"USE CATALOG {active_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.raw_landing")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.default")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.default.checkpoints")

# COMMAND ----------
# 2. Setup Sample Raw Landing Table with JSON & Nested Structs
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

# COMMAND ----------
# 3. Insert Sample Records (Valid records, malformed PII, and VARIANT payloads)
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

# COMMAND ----------
# 4. Define Declarative Pipeline Configuration
import yaml
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.streaming import run_pipeline

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
  checkpoint_location: f"/Volumes/{active_catalog}/default/checkpoints/subscriptions"

gold:
  catalog: "{active_catalog}"
  schema: "default"
  table: "dim_subscriptions"
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
    - name: "valid_status"
      expr: "status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED')"
      action: "quarantine"

  fields:
    - name: "subscription_id"
      type: "string"
      expr: "subscriptionId"
      is_primary_key: true
      comment: "Unique surrogate key for subscription contract"
    - name: "plan_tier"
      type: "string"
      expr: "plan.tier"
      comment: "Subscription pricing tier"
    - name: "status"
      type: "string"
      expr: "status"
      comment: "Subscription lifecycle status"
    - name: "mrr_amount"
      type: "double"
      expr: "cast(plan.amountCents as double) / 100.0"
      comment: "Monthly Recurring Revenue normalized in USD"
    - name: "started_at"
      type: "timestamp"
      expr: "startedOn"
      comment: "Contract start timestamp"
    - name: "subscriber_account_id"
      type: "string"
      expr: "xxhash64(cast(subscriberId as string))"
      comment: "Pseudonymized subscriber foreign key"

    # Databricks Native VARIANT column
    - name: "device_metadata"
      type: "variant"
      expr: "raw_device_headers"
      comment: "Device and client headers stored in native binary VARIANT"

pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "{active_catalog}"
    target_schema: "default"
    target_table: "dim_subscriber_profiles_pii"
    unique_keys: ["subscriber_account_id"]
    foreign_key_expr: "xxhash64(cast(subscriberId as string))"
    foreign_key_name: "subscriber_account_id"
    cluster_by: ["created_at"]
    fields:
      - name: "subscriber_email"
        type: "string"
        expr: "subscriber.email"
        comment: "Direct PII customer email"
      - name: "contact_name"
        type: "string"
        expr: "subscriber.contactName"
        comment: "Full customer contact name"
      - name: "created_at"
        type: "timestamp"
        expr: "startedOn"
"""

config = EntityPipelineConfig.model_validate(yaml.safe_load(config_yaml))

# COMMAND ----------
# 5. Execute Streaming Pipeline & Display Results
run_pipeline(spark, config)

print("=== GOLD CLEAN DIMENSION ===")
display(spark.table(f"{active_catalog}.default.dim_subscriptions"))

print("=== SEGREGATED PII TABLE ===")
display(spark.table(f"{active_catalog}.default.dim_subscriber_profiles_pii"))
```

---

## 3. Running Historical Backfill in Notebook

To run a historical backfill directly inside Databricks Community Edition without altering checkpoint offsets:

```python
from lakehouse_engine.core.backfill import run_backfill

run_backfill(
    spark=spark,
    config=config,
    start_date="2026-01-01",
    end_date="2026-03-31",
    date_column="started_at",
    reset_checkpoint=False
)
```

---

## 4. Running 100% Locally Without Databricks

You can run the full suite and tests on your laptop without any cloud or Databricks account:

```bash
# 1. Install dependencies
task install

# 2. Run unit tests across all engine domains
task test

# 3. Run Autonomous DLQ Triage on sample records
task dlq:triage SAMPLE=configs/examples/sample_dlq_records.json

# 4. Start MCP Server for AI coding assistants
task mcp:serve
```

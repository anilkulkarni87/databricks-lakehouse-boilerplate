# Running on Databricks Free Tier (Community Edition) & Local PySpark

This guide explains how to run, test, and demonstrate this entire framework on **Databricks Community Edition (100% free)** or **locally on your laptop** without incurring cloud infrastructure costs.

---

## 1. Free Tier (Community Edition) vs. Enterprise Differences

| Feature | Databricks Enterprise / Paid | Community Edition (Free Tier) | How This Framework Adapts |
| :--- | :--- | :--- | :--- |
| **Catalog** | Unity Catalog (3-level namespace: `cat.schema.tbl`) | Hive Metastore (2-level namespace: `schema.tbl` or `default.tbl`) | Set `catalog: "hive_metastore"` or `catalog: ""` in YAML |
| **Cluster** | Multi-node or dedicated single-node spot instances | Single micro-cluster (15GB memory, shared driver/worker) | Fits perfectly: our jobs are single-node optimized! |
| **Job API** | Automated Databricks Workflows via Terraform | No programmatic Job API scheduler | Run via Interactive Notebook using `%run` or `run_pipeline(spark, config)` |
| **Storage** | S3 / ADLS / GCS Cloud Storage | DBFS / Local storage | Use `dbfs:/tmp/checkpoints` |

---

## 2. Running on Databricks Community Edition in 3 Steps

### Step 1: Sign Up & Create a Cluster
1. Sign up for free at [community.cloud.databricks.com](https://community.cloud.databricks.com/).
2. Create a free single-node cluster (select Databricks Runtime 14.x or 15.x).

### Step 2: Upload Package Wheel
1. Build the wheel locally:
   ```bash
   task build
   ```
2. In Databricks Community Edition, navigate to **Compute** $\rightarrow$ select your cluster $\rightarrow$ **Libraries** $\rightarrow$ **Install New** $\rightarrow$ **Upload Python Whl** $\rightarrow$ select `dist/lakehouse_engine-0.1.0-py3-none-any.whl`.

### Step 3: Run via Databricks Notebook
Create a new Python notebook on your cluster and paste the following self-contained script:

```python
# -------------------------------------------------------------
# 1. Setup Sample Raw Landing Table with JSON & Nested Structs
# -------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS raw_landing")
spark.sql("""
CREATE TABLE IF NOT EXISTS raw_landing.silver_subscription_events (
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

# -------------------------------------------------------------
# 2. Insert Sample Records (Valid and Malformed)
# -------------------------------------------------------------
spark.sql("""
INSERT INTO raw_landing.silver_subscription_events VALUES (
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
  '{\"user_agent\": \"Mozilla/5.0\", \"ip\": \"192.168.1.1\", \"dark_mode\": true}'
)
""")

# -------------------------------------------------------------
# 3. Define Declarative Configuration with Governance & VARIANT
# -------------------------------------------------------------
import yaml
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.streaming import run_pipeline

config_yaml = """
version: "1.0"
entity_name: "subscriptions"

source:
  catalog: "spark_catalog"
  schema: "raw_landing"
  table: "silver_subscription_events"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "dbfs:/tmp/checkpoints/subscriptions"

gold:
  catalog: "spark_catalog"
  schema: "default"
  table: "dim_subscriptions"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  prune_column: "started_at"

  # Phase 1: Dead-Letter Queue & Micro-batch Audit
  enable_dlq: true
  dlq_table: "default.dim_subscriptions_dlq"
  track_rescued_data: true

  # Phase 2: Declarative Quality Expectations
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

    # Phase 3: Databricks Native VARIANT column
    - name: "device_metadata"
      type: "variant"
      expr: "raw_device_headers"
      comment: "Device and client headers stored in native binary VARIANT"

pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "spark_catalog"
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

# -------------------------------------------------------------
# 4. Execute Streaming Pipeline & Display Results
# -------------------------------------------------------------
run_pipeline(spark, config)

print("=== GOLD CLEAN DIMENSION ===")
display(spark.table("default.dim_subscriptions"))

print("=== SEGREGATED PII TABLE ===")
display(spark.table("default.dim_subscriber_profiles_pii"))

print("=== MICRO-BATCH AUDIT LOG ===")
display(spark.table("default._pipeline_microbatch_audit"))
```

---

## 3. Running Historical Backfill in Notebook

To run a backfill directly inside Databricks Community Edition:

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

You can run the full suite and tests on your laptop without any cloud account:

```bash
# 1. Install dependencies
task install

# 2. Run unit tests with local delta-spark engine
task test

# 3. Run MCP server for AI tools
task mcp:serve
```

# Running on Databricks Free Tier (Community Edition) & Local PySpark

This guide explains how to run, test, and demonstrate this entire framework on **Databricks Community Edition (100% free)** or **locally on your laptop** without incurring cloud infrastructure costs.

---

## 1. Free Tier (Community Edition) vs. Enterprise Differences

| Feature | Databricks Enterprise / Paid | Community Edition (Free Tier) | How This Framework Adapts |
| :--- | :--- | :--- | :--- |
| **Catalog** | Unity Catalog (3-level namespace: `cat.schema.tbl`) | Hive Metastore (2-level namespace: `schema.tbl` or `default.tbl`) | Set `catalog: "hive_metastore"` or `catalog: ""` in YAML |
| **Cluster** | Multi-node or dedicated single-node spot instances | Single micro-cluster (15GB memory, shared driver/worker) | Fits perfectly: our jobs are single-node optimized! |
| **Job API** | Automated Databricks Workflows via Terraform | No programmatic Job API scheduler | Run via Interactive Notebook using `%run` or `local_runner.py` |
| **Storage** | S3 / ADLS / GCS Cloud Storage | DBFS / Local storage | Use `dbfs:/checkpoints` or `/tmp/checkpoints` |

---

## 2. Running on Databricks Community Edition in 3 Steps

### Step 1: Sign Up & Create a Cluster
1. Sign up for free at [community.cloud.databricks.com](https://community.cloud.databricks.com/).
2. Create a free single-node cluster (select DBR 14.x or 15.x).

### Step 2: Upload Package Wheel
1. Build the wheel locally:
   ```bash
   task build
   ```
2. In Databricks Community Edition, navigate to **Compute** $\rightarrow$ select your cluster $\rightarrow$ **Libraries** $\rightarrow$ **Install New** $\rightarrow$ **Upload Python Whl** $\rightarrow$ select `dist/lakehouse_engine-0.1.0-py3-none-any.whl`.

### Step 3: Run via Databricks Notebook
Create a new Python notebook on your cluster and paste:

```python
# Create sample landing stream table
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
  subscriber STRUCT<email: STRING, contactName: STRING, phone: STRING, taxIdentifier: STRING, country: STRING, postalCode: STRING>
) USING DELTA
""")

# Insert a sample event record
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
  named_struct('email', 'alex.miller@example.com', 'contactName', 'Alex Miller', 'phone', '+1-555-0123', 'taxIdentifier', 'US-9912', 'country', 'US', 'postalCode', '98101')
)
""")

# Execute the pipeline with your config
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
  fields:
    - name: "subscription_id"
      type: "string"
      expr: "subscriptionId"
    - name: "plan_tier"
      type: "string"
      expr: "plan.tier"
    - name: "mrr_amount"
      type: "double"
      expr: "cast(plan.amountCents as double) / 100.0"
    - name: "started_at"
      type: "timestamp"
      expr: "startedOn"
    - name: "subscriber_account_id"
      type: "string"
      expr: "xxhash64(cast(subscriberId as string))"
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
      - name: "created_at"
        type: "timestamp"
        expr: "startedOn"
"""

config = EntityPipelineConfig.model_validate(yaml.safe_load(config_yaml))
run_pipeline(spark, config)

# Verify results!
display(spark.table("default.dim_subscriptions"))
display(spark.table("default.dim_subscriber_profiles_pii"))
```

---

## 3. Running 100% Locally Without Databricks

You can run the full pipeline on your laptop without any cloud or Databricks account:

```bash
# 1. Install dependencies
task install

# 2. Run unit tests with local delta-spark engine
task test
```

The tests use `chispa` and an in-memory local Delta Spark session to execute transformations and verify PII extraction logic.

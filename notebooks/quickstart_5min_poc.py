# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Lakehouse Engine: 5-Minute Interactive POC & Quickstart
# MAGIC 
# MAGIC Welcome to the **Lakehouse Engine** proof-of-concept walkthrough!
# MAGIC 
# MAGIC In this 5-minute interactive notebook, you will evaluate:
# MAGIC 1. **Declarative Streaming Ingestion**: Automated schema casting, Liquid Clustering, and PII isolation via `xxhash64`.
# MAGIC 2. **Native Binary `VARIANT` Columns**: Querying semi-structured JSON client headers with zero string-parsing overhead.
# MAGIC 3. **Circuit-Breaker Expectations**: Catching bad records and routing them to a Dead-Letter Queue (DLQ).
# MAGIC 4. **Autonomous Self-Healing DLQ Triage**: Automatically diagnosing root cause schema drift and generating YAML patches.
# MAGIC 5. **Idempotent Historical Backfill**: Restating historical date windows without resetting streaming checkpoints.
# MAGIC 6. **Databricks Genie AI Semantic Compiler**: Compiling certified system prompts and informational SQL constraints.
# MAGIC 7. **Fail-Fast Safety Guardrails & Data Contracts**: Pre-flight linting and SLA circuit breakers.
# MAGIC 8. **Production Storage Maintenance & Governance Tags**: Liquid Clustering file compaction (`OPTIMIZE`), `VACUUM`, and Unity Catalog compliance tags.
# MAGIC 
# MAGIC *Click **"Run All"** to execute the complete end-to-end flow!*

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 1: Install Engine Wheel
# MAGIC Install the engine wheel package on the cluster and streaming worker processes.

# COMMAND ----------
# Update path to where your wheel artifact is stored in Unity Catalog Volumes
# MAGIC %pip install /Volumes/main/default/library/lakehouse_engine-0.2.0-py3-none-any.whl --force-reinstall

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 2: Resolve Active Unity Catalog & Setup Schemas
# MAGIC Dynamically detects your active catalog (`main`, `dev_lakehouse`, or custom sandbox).

# COMMAND ----------
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
active_catalog = "main" if "main" in catalogs else catalogs[0]
print(f"✅ Active Unity Catalog: '{active_catalog}'")

spark.sql(f"USE CATALOG {active_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.raw_landing")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.default")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.default.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.default.library")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 3: Seed Landing Events Table
# MAGIC Seeds a mock Silver landing table containing nested structs and JSON device headers.

# COMMAND ----------
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

# Seed clean baseline record
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
print("✅ Baseline Silver event seeded successfully!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 4: Define Declarative Pipeline Configuration
# MAGIC Entire pipeline defined in declarative YAML. No hardcoded PySpark transformation logic!

# COMMAND ----------
from lakehouse_engine.core.config_loader import load_pipeline_config
from lakehouse_engine.core.streaming import run_pipeline

config_yaml = f"""
version: "1.0"
entity_name: "subscriptions"

# Fail-Fast Production Safety Guardrails
guardrails:
  enforce_primary_key_order_by: true
  require_prune_column_on_merge: true
  pii_leakage_detection: "strict"

# Data Contract SLA Thresholds
contract:
  schema_drift_policy: "fail_on_incompatible"
  max_quarantine_rate_percent: 75.0   # SLA threshold: breach if quarantine rate > 75%
  quarantine_action: "warn"           # 'warn' in POC allows DLQ inspection; use 'fail_batch' in prod for strict SLA halts
  enforce_not_null_pk: true

source:
  catalog: "{active_catalog}"
  schema: "raw_landing"
  table: "silver_subscription_events"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "/Volumes/{active_catalog}/default/checkpoints/subscriptions"
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
  tags:
    data_classification: "internal"
    tier: "mission-critical"
  enable_dlq: true
  dlq_table: "{active_catalog}.default.dim_subscriptions_dlq"
  track_rescued_data: true

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
      description: "Subscription pricing tier"
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
    - name: "device_metadata"
      type: "variant"
      expr: "raw_device_headers"
      description: "Device headers stored in native binary VARIANT"

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
    tags:
      data_classification: "restricted_pii"
      compliance_scope: "gdpr_ccpa" 
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

from lakehouse_engine.core.guardrails import lint_pipeline_config

config = load_pipeline_config(config_yaml)

# Execute Pre-Flight Config Guardrail Linting
lint_report = lint_pipeline_config(config)
print(lint_report.format_console_report())

if not lint_report.is_valid:
    raise ValueError("Config guardrails failed! Halting pipeline execution.")

print("✅ Configuration validated and approved for streaming ingestion!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 5: Execute Ingestion & Inspect Clean Layers

# COMMAND ----------
# 1. Run micro-batch streaming pipeline
run_pipeline(spark, config)

# 2. View Curated Gold Dimension
print("=== GOLD CLEAN DIMENSION ===")
display(spark.table(f"{active_catalog}.default.dim_subscriptions"))

# 3. Query Native Binary VARIANT Column (Photon-accelerated)
print("=== QUERYING NATIVE VARIANT COLUMN ===")
display(spark.sql(f"""
  SELECT 
    subscription_id,
    plan_tier,
    mrr_amount,
    device_metadata:ip::string AS client_ip,
    device_metadata:dark_mode::boolean AS dark_mode_active
  FROM {active_catalog}.default.dim_subscriptions
"""))

# 4. View Segregated PII Dimension
print("=== ISOLATED COMPLIANCE PII TABLE ===")
display(spark.table(f"{active_catalog}.default.dim_subscriber_profiles_pii"))

# 5. View Operational Telemetry Audit Log
print("=== OPERATIONAL TELEMETRY LOG ===")
display(spark.table(f"{active_catalog}.default.pipeline_batch_audit"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 6: Inject Schema Drift & Expectation Violations
# MAGIC Inject two malformed records:
# MAGIC * **Record A**: Status `'PENDING_REVIEW'` (violates `valid_status` expectation).
# MAGIC * **Record B**: Primary key `subscriptionId` is `NULL`.

# COMMAND ----------
spark.sql(f"""
INSERT INTO {active_catalog}.raw_landing.silver_subscription_events VALUES 
-- Valid Record 1 (merges successfully into Gold)
(
  'sub_0006',
  named_struct('tier', 'Enterprise', 'billingInterval', 'annual', 'amountCents', 120000),
  'USD',
  'ACTIVE',
  false,
  false,
  0,
  current_timestamp(),
  current_timestamp(),
  'cust_106',
  named_struct('email', 'valid1@example.com', 'contactName', 'Valid User 1', 'phone', '+1-555-0106', 'taxIdentifier', 'US-106', 'country', 'US', 'postalCode', '94105'),
  '{{\"user_agent\": \"Desktop\", \"dark_mode\": false}}'
),
-- Valid Record 2 (merges successfully into Gold)
(
  'sub_0007',
  named_struct('tier', 'Pro', 'billingInterval', 'monthly', 'amountCents', 4900),
  'USD',
  'ACTIVE',
  false,
  false,
  0,
  current_timestamp(),
  current_timestamp(),
  'cust_107',
  named_struct('email', 'valid2@example.com', 'contactName', 'Valid User 2', 'phone', '+1-555-0107', 'taxIdentifier', 'US-107', 'country', 'US', 'postalCode', '94105'),
  '{{\"user_agent\": \"Desktop\", \"dark_mode\": true}}'
),
-- Malformed Record A: Violates valid_status expectation ('PENDING_REVIEW') -> Routed to DLQ
(
  'sub_9999',
  named_struct('tier', 'Pro', 'billingInterval', 'monthly', 'amountCents', 4900),
  'USD',
  'PENDING_REVIEW',  -- Malformed status!
  false,
  true,
  1,
  current_timestamp(),
  current_timestamp(),
  'cust_999',
  named_struct('email', 'drift@example.com', 'contactName', 'Drift User', 'phone', '+1-555-9999', 'taxIdentifier', 'US-99', 'country', 'US', 'postalCode', '10001'),
  '{{\"user_agent\": \"Mobile-App\", \"dark_mode\": false}}'
),
-- Malformed Record B: Missing Primary Key (NULL) -> Routed to DLQ
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
print("=== QUARANTINED DEAD-LETTER QUEUE (DLQ) RECORDS ===")
display(spark.table(f"{active_catalog}.default.dim_subscriptions_dlq"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 7: Autonomous Self-Healing DLQ Triage
# MAGIC Automatically inspects quarantined records, determines failure root causes, and generates a self-healing YAML remediation patch.

# COMMAND ----------
from lakehouse_engine.core.triage import analyze_quarantined_records

# Fetch quarantined records directly from the Delta DLQ table
dlq_rows = [row.asDict(recursive=True) for row in spark.table(f"{active_catalog}.default.dim_subscriptions_dlq").collect()]

# Execute Autonomous Triage Analysis
report = analyze_quarantined_records(config, dlq_rows)
print(report.format_console_report())

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 8: Idempotent Historical Batch Backfill
# MAGIC Execute historical backfills without altering or resetting streaming checkpoint offsets.

# COMMAND ----------
from lakehouse_engine.core.backfill import run_backfill

run_backfill(
    spark=spark,
    config=config,
    start_date="2026-01-01",
    end_date="2026-12-31",
    date_column="started_at",
    reset_checkpoint=False
)
print("✅ Historical backfill completed with Delta idempotency!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 9: Databricks Genie AI Adoption
# MAGIC Auto-compile Genie Space System Prompts and Unity Catalog informational constraints (`NOT ENFORCED`).

# COMMAND ----------
from lakehouse_engine.core.genie import generate_genie_space_documentation

genie_doc = generate_genie_space_documentation([config])
print(genie_doc)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 10: Production Storage Maintenance (Liquid Compaction & Vacuum)
# MAGIC Execute automated storage maintenance (Liquid Clustering file compaction and deleted file purging) across Gold, PII, and DLQ tables.

# COMMAND ----------
from lakehouse_engine.core.maintenance import run_table_maintenance
import json

maintenance_report = run_table_maintenance(spark, config, retention_hours=168)
print("=== DELTA STORAGE MAINTENANCE REPORT ===")
print(json.dumps(maintenance_report, indent=2))

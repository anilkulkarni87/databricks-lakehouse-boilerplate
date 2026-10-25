# Practical Examples & Walkthroughs

**[About](index.md)** | **[Azure Guide](azure_production_guide.md)** | **[Production Guide](production_adoption_guide.md)** | **[Limitations](limitations.md)** | **[Pros & Cons](pros_and_cons.md)** | **[Examples](examples.md)** | **[GitHub Repository](https://github.com/anilkulkarni87/databricks-lakehouse-boilerplate)**

---

All examples below are derived directly from working test fixtures, CLI tools, and configurations verified in the repository.

---

## 1. Complete Declarative Entity Configuration (`subscriptions.yml`)

The following YAML demonstrates a complete entity definition with environment token interpolation, Liquid Clustering, PII isolation, data quality circuit breakers, and native `VARIANT` columns:

```yaml
version: "1.0"
entity_name: "subscriptions"

# Secret Scope Integration
secrets:
  slack_webhook: "{{secrets/lakehouse_secrets/slack_webhook}}"

# Fail-Fast Safety Guardrails
guardrails:
  enforce_primary_key_order_by: true
  require_prune_column_on_merge: true
  pii_leakage_detection: "strict"

# Data Contract SLA Thresholds
contract:
  schema_drift_policy: "fail_on_incompatible"
  max_quarantine_rate_percent: 5.0
  quarantine_action: "fail_batch"

source:
  catalog: "${CATALOG:-main}"
  schema: "raw_landing"
  table: "silver_subscription_events"
  payload_column: "payload"
  payload_type: "struct"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "${CHECKPOINT_ROOT:-/Volumes/main/default}/checkpoints/subscriptions"
  audit_table: "${CATALOG:-main}.default.pipeline_batch_audit"

gold:
  catalog: "${CATALOG:-main}"
  schema: "default"
  table: "dim_subscriptions"
  table_comment: "Curated SaaS subscription dimension with contract metrics and pseudonymized identity"
  table_type: "dimension"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  prune_column: "started_at"

  # Zero Data Loss: Dead-Letter Queue & Rescued Data
  enable_dlq: true
  dlq_table: "${CATALOG:-main}.default.dim_subscriptions_dlq"
  track_rescued_data: true

  # Declarative Data Quality Circuit Breakers
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

    # Hardware-Accelerated Native Binary VARIANT Column
    - name: "device_metadata"
      type: "variant"
      expr: "raw_device_headers"
      description: "Device and client headers stored in native binary VARIANT"

pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "${CATALOG:-main}"
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
```

---

## 2. Querying Native Binary `VARIANT` Columns in Databricks SQL

Once ingested, the `device_metadata` column is stored in native binary `VARIANT` format. Analysts query nested attributes without JSON parsing overhead:

```sql
SELECT 
  subscription_id,
  plan_tier,
  mrr_amount,
  device_metadata:ip::string AS client_ip,
  device_metadata:dark_mode::boolean AS is_dark_mode_active
FROM main.default.dim_subscriptions
WHERE device_metadata:ip IS NOT NULL;
```

---

## 3. Autonomous DLQ Triage CLI (`triage-dlq`)

When records fail an expectation or miss a primary key, they land in `dim_subscriptions_dlq`. Run the autonomous triage engine locally or in Databricks:

```bash
# Run triage on quarantined records with sample data
task dlq:triage CONFIG=configs/examples/subscriptions.yml SAMPLE=configs/examples/sample_dlq_records.json
```

### Output Diagnostic RCA Report:
```text
================================================================================
           AUTONOMOUS DEAD-LETTER QUEUE (DLQ) TRIAGE REPORT
================================================================================
Entity Analyzed : subscriptions
Total DLQ Rows  : 2
Unique Clusters : 2
--------------------------------------------------------------------------------
Cluster #1: EXPECTATION_VIOLATION: valid_status
  - Quarantined Count: 1 rows (50.0%)
  - Failing Rule     : status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED')
  - New Categorical Values Discovered:
      * 'PENDING_REVIEW' (1 occurrences)
  - Remediation Action:
      Expand expectation 'valid_status' allowed values to include 'PENDING_REVIEW'.

Cluster #2: NULL_PRIMARY_KEY: subscription_id
  - Quarantined Count: 1 rows (50.0%)
  - Failing Field    : subscription_id
  - Schema Drift Detected in '_rescued_payload':
      * Found candidate key 'sub_id' with 100% non-null presence!
  - Remediation Action:
      Update field 'subscription_id' expr to: coalesce(subscriptionId, sub_id)
================================================================================
```

### Applying the Self-Healing Fix Automatically:
```bash
python3 -m lakehouse_engine.cli triage-dlq -c configs/examples/subscriptions.yml -s configs/examples/sample_dlq_records.json --apply-fix
```
The engine automatically patches the YAML configuration using a unified diff.

---

## 4. Historical Batch Backfilling (`run_backfill`)

Execute an idempotent historical backfill without modifying streaming checkpoint offsets:

```bash
task backfill CONFIG=configs/examples/subscriptions.yml START=2026-01-01 END=2026-06-30
```

* **Behavior**: Reads the specified date window using partition pruning, performs in-batch deduplication, merges into `dim_subscriptions`, and leaves the streaming checkpoint untouched.

---

## 5. Compiling Databricks Genie Space Documentation

Compile your entity YAMLs into a Databricks Genie Space prompt and informational SQL constraints:

```bash
task genie:export
```

### Generated Unity Catalog Informational Constraints (`NOT ENFORCED`):
```sql
ALTER TABLE main.default.dim_subscriptions 
  ADD CONSTRAINT pk_dim_subscriptions_subscription_id PRIMARY KEY (subscription_id) NOT ENFORCED;

ALTER TABLE main.default.dim_subscriptions 
  ADD CONSTRAINT fk_dim_subscriptions_subscriber_account_id 
  FOREIGN KEY (subscriber_account_id) REFERENCES main.default.dim_subscriber_profiles_pii(subscriber_account_id) NOT ENFORCED;
```

---

## 6. Model Context Protocol (MCP) Server for AI Assistants

Start the native MCP server to allow AI coding tools (Cursor, Claude, Antigravity) to inspect, scaffold, and triage lakehouse pipelines:

```bash
task mcp:serve
```

### Available Tools Exposed via MCP:
* `validate_pipeline_config`: Validates YAML syntax and constraints against Pydantic models.
* `list_pipeline_entities`: Lists all configured entities in the repository.
* `triage_dlq`: Runs autonomous self-healing analysis over quarantined records.
* `scaffold_pipeline`: Auto-generates a valid pipeline configuration from a schema specification.
* `get_genie_space_documentation`: Generates Genie AI prompts for configured entities.

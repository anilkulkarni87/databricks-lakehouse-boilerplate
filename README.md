# Databricks Lakehouse Metadata-Driven ETL Boilerplate

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-brightgreen.svg)](pyproject.toml)
[![Databricks](https://img.shields.io/badge/Databricks-DBR%2014%2F15%2B-orange.svg)](terraform/modules/databricks_job/variables.tf)
[![Terraform](https://img.shields.io/badge/Terraform-1.5%2B-purple.svg)](terraform/main.tf)

A production-ready, metadata-driven framework for building scalable **Medallion Lakehouse Architectures** on Databricks.

Instead of writing bespoke, repetitive pipeline code and compiling monolithic binaries for every new entity, this framework uses **declarative YAML definitions** evaluated by a generic, optimized PySpark streaming engine with **automated PII segregation**, **Databricks Liquid Clustering**, and **Terraform IaC**.

Works identically across **Databricks Enterprise (Unity Catalog)**, **Databricks Community Edition (100% Free Tier)**, and **local laptop PySpark environments**.

---

## 🎯 Architecture Diagram

### 1. Medallion Data Flow & Automated PII Split

```mermaid
flowchart TD
    subgraph SilverLayer ["Silver Ingestion Stream (Landing Table)"]
        StreamSrc["silver_subscription_events<br/>(Kafka / CDC / Airbyte landing)"]
    end

    subgraph Engine ["Generic Lakehouse Engine (Wheel / CLI)"]
        MicroBatch["Structured Streaming Micro-Batch<br/>(Trigger.AvailableNow)"]
        Parser["Declarative YAML Evaluator<br/>(Pydantic validated)"]
        Dedup["In-Batch Window Deduplication<br/>(row_number() == 1 over primary keys)"]
    end

    subgraph GoldDW ["Curated Gold Analytics Layer"]
        GoldTable[("dim_subscriptions<br/>saas_dw.subscription_analytics<br/>- Liquid Clustered on started_at<br/>- Date-Pruned Idempotent MERGE<br/>- Deterministic Hash: subscriber_account_id")]
    end

    subgraph PIILayer ["Isolated PII Security Layer"]
        PIITable[("dim_subscriber_profiles<br/>saas_dw.subscription_analytics_pii<br/>- Foreign Key: subscriber_account_id<br/>- Restricted Compliance Access Group<br/>- Dynamic Column Masking UDF (mask_email)")]
    end

    StreamSrc --> MicroBatch
    MicroBatch --> Parser
    Parser --> Dedup
    Dedup -->|"Extract non-PII metrics"| GoldTable
    Dedup -->|"Route sensitive identity fields"| PIITable
    GoldTable -.->|"Secure JOIN via xxhash64"| PIITable
```

### 2. Micro-Batch Execution Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Spark as Spark Structured Stream
    participant Engine as Lakehouse Engine
    participant Gold as Target Delta Table
    participant PII as Target PII Table

    Spark->>Engine: Process Micro-Batch (Trigger.AvailableNow)
    Engine->>Engine: Cache batch & inspect incoming dates
    Engine->>Engine: Window dedup across unique_keys
    Engine->>Gold: Auto-create table with Liquid Clustering (if not exists)
    Engine->>Gold: Pruned MERGE: (target.date IN (batch.dates) AND target.id = source.id)
    Engine->>PII: Extract PII fields & compute xxhash64 FK
    Engine->>PII: Auto-create PII table & execute idempotent MERGE
    Engine->>Spark: Commit checkpoint offset & release cache
```

---

## 🤖 Context Files for AI Agents (Cursor, Claude, Copilot)

This repository includes full architectural context specifications in the [`context/`](context/) directory to allow LLMs to generate compliant configs and code without hallucinations:
- **[`CLAUDE.md`](CLAUDE.md)**: Agent rules, command cheat-sheet, and code style.
- **[`context/architecture_overview.md`](context/architecture_overview.md)**: Medallion design and declarative metadata concepts.
- **[`context/delta_lake_engine_norms.md`](context/delta_lake_engine_norms.md)**: Liquid clustering rules, in-batch deduplication, and pruned MERGE patterns.
- **[`context/pii_segregation_patterns.md`](context/pii_segregation_patterns.md)**: PII isolation, deterministic hash linkage (`xxhash64`), and dynamic column masking UDFs.
- **[`context/databricks_free_tier_guide.md`](context/databricks_free_tier_guide.md)**: Complete guide for Databricks Community Edition and local environments.

---

## 📁 Repository Structure

```text
├── configs/
│   ├── templates/
│   │   └── entity_template.yml    # Scaffolding template for new entities
│   └── examples/
│       └── subscriptions.yml      # Anonymized SaaS subscription + PII profile example
├── context/                       # In-depth architectural context for LLMs & Engineers
│   ├── architecture_overview.md
│   ├── delta_lake_engine_norms.md
│   ├── pii_segregation_patterns.md
│   └── databricks_free_tier_guide.md
├── src/
│   └── lakehouse_engine/
│       ├── cli.py                 # Databricks workflow entry point CLI
│       ├── models/
│       │   └── config.py          # Pydantic schema validation for configs
│       └── core/
│           ├── delta_ops.py       # Liquid clustering, in-batch dedup, pruned MERGE
│           ├── pii.py             # PII extraction and isolation logic
│           ├── streaming.py       # Micro-batch streaming coordinator
│           └── transformer.py     # SQL expression and attribute evaluator
├── terraform/
│   ├── env_conf/                  # Per-environment variables (dev, prod)
│   ├── modules/
│   │   ├── databricks_job/        # Single-node spot workflow definition
│   │   └── unity_governance/      # Catalog, PII schemas, masking UDFs, ACLs
│   ├── main.tf                    # Root composition
│   └── variables.tf
├── tests/
│   ├── conftest.py                # Local PySpark session fixture
│   └── test_config.py             # Pydantic YAML schema validation tests
├── CLAUDE.md                      # AI agent coding guidelines & rules
├── pyproject.toml                 # Modern PEP 621 Python packaging
├── Taskfile.yml                   # Unified developer automation commands
└── README.md
```

---

## ⚡ Quickstart: Adding a New Entity in 3 Minutes

### Step 1: Create Entity Config
Copy [configs/templates/entity_template.yml](configs/templates/entity_template.yml) to `configs/examples/subscriptions.yml`:

```yaml
version: "1.0"
entity_name: "subscriptions"

source:
  catalog: "raw_landing"
  schema: "event_stream"
  table: "silver_subscription_events"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 2000
  checkpoint_location: "dbfs:/checkpoints/lakehouse_engine/subscriptions"

# Non-PII Gold Curated Table
gold:
  catalog: "saas_dw"
  schema: "subscription_analytics"
  table: "dim_subscriptions"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  prune_column: "started_at"
  fields:
    - name: "subscription_id"
      type: "string"
      expr: "payload.subscriptionId"
    - name: "plan_tier"
      type: "string"
      expr: "payload.plan.tier"
    - name: "mrr_amount"
      type: "double"
      expr: "cast(payload.plan.amountCents as double) / 100.0"
    - name: "started_at"
      type: "timestamp"
      expr: "cast(payload.startedOn as timestamp)"
    - name: "subscriber_account_id"
      type: "string"
      expr: "xxhash64(cast(payload.subscriberId as string))"

# Automated PII Isolation: Route contact data to restricted _pii schema
pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "saas_dw"
    target_schema: "subscription_analytics_pii"
    target_table: "dim_subscriber_profiles"
    unique_keys: ["subscriber_account_id"]
    foreign_key_expr: "xxhash64(cast(payload.subscriberId as string))"
    foreign_key_name: "subscriber_account_id"
    cluster_by: ["created_at"]
    fields:
      - name: "subscriber_email"
        type: "string"
        expr: "payload.subscriber.email"
        mask_policy: "mask_email"
      - name: "billing_phone"
        type: "string"
        expr: "payload.subscriber.phone"
```

### Step 2: Validate Locally
Run the automated test suite:
```bash
task test
```

### Step 3: Deploy via Terraform
```bash
task tf:plan ENV=dev
task tf:apply ENV=dev
```

---

## 🆓 Using with Databricks Free Tier (Community Edition)

You can run and showcase this entire framework on **Databricks Community Edition (100% Free)**:

1. **Sign up for free** at [community.cloud.databricks.com](https://community.cloud.databricks.com/).
2. **Build the wheel**:
   ```bash
   task build
   ```
3. **Upload the Wheel**: In your free Community cluster, navigate to **Compute** $\rightarrow$ **Libraries** $\rightarrow$ **Install New** $\rightarrow$ **Python Whl** $\rightarrow$ select `dist/lakehouse_engine-0.1.0-py3-none-any.whl`.
4. **Run via Notebook**: Create a Python notebook and execute:
   ```python
   import yaml
   from lakehouse_engine.models.config import EntityPipelineConfig
   from lakehouse_engine.core.streaming import run_pipeline

   # Read the config and run
   with open("/Workspace/Shared/subscriptions.yml") as f:
       config = EntityPipelineConfig.model_validate(yaml.safe_load(f))
   
   run_pipeline(spark, config)
   ```
*(For complete step-by-step notebook scripts and sample data generator, see [context/databricks_free_tier_guide.md](context/databricks_free_tier_guide.md)).*

---

## 🛠️ Developer Commands (`Taskfile.yml`)

| Command | Description |
| :--- | :--- |
| `task install` | Installs local Python environment and dev dependencies |
| `task test` | Runs unit tests with `pytest` |
| `task lint` | Runs `ruff` linter and `mypy` type checking |
| `task format` | Formats code with `ruff` |
| `task build` | Builds the lightweight `.whl` package to `dist/` |
| `task tf:plan ENV=dev` | Runs `terraform plan` against specified environment |
| `task tf:apply ENV=prod` | Deploys Terraform resources to specified environment |

---

## 📄 License
Licensed under the [Apache-2.0 License](LICENSE).

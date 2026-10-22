# CLAUDE.md - AI Agent Context & Guidelines

This document provides foundational architecture rules and workflows for AI agents (Claude, Cursor, Copilot, Antigravity) working on this codebase.

---

## 🚀 Common Commands

Builds, tests, and deployments use [go-task](https://taskfile.dev/) (`Taskfile.yml`):

```bash
task install            # Install Python package in editable mode with dev dependencies
task test               # Run pytest suite across tests/
task lint               # Run ruff check and mypy type checking
task format             # Auto-format with ruff
task build              # Package wheel into dist/ for Databricks deployment
task tf:plan ENV=dev    # Terraform plan against target environment
task tf:apply ENV=dev   # Terraform apply against target environment
```

---

## 🏛️ Architectural Principles

1. **Declarative Metadata-First**:
   - Never write bespoke Scala or Python pipeline files for a new entity.
   - All entity transformations, mappings, and schemas are defined in `configs/*.yml` and validated by Pydantic (`src/lakehouse_engine/models/config.py`).
2. **Medallion Ingestion Pattern**:
   - **Silver**: Source table storing payload data + ingestion tracking columns (`_ingested_at`, `stream_name`).
   - **Gold**: Curated, schema-typed, non-PII dimension/fact tables (`gold` block in YAML).
   - **PII Layer**: Sensitive attributes are extracted into separate tables in a restricted `_pii` schema (`pii_splits` block in YAML).
3. **Delta Lake Best Practices**:
   - **Liquid Clustering**: Tables must specify `.clusterBy(...)` using business event timestamps (`created_at`, `started_at`). Never use legacy directory partitions.
   - **In-Batch Deduplication**: Always deduplicate inside micro-batches before executing a MERGE to avoid duplicate-key merge failures.
   - **Pruned Idempotent MERGE**: MERGE join conditions must include a partition/date filter (`to_date(target.col) IN (...)`) derived from the incoming batch to prevent full-table scans.
4. **Zero-Blast Radius**:
   - Adding a new entity or extending fields should only touch `configs/<entity>.yml`.
   - Never rebuild binaries or re-deploy existing jobs for simple schema extensions.

---

## 📁 Repository Reference Map

- `configs/templates/entity_template.yml`: Reference schema for all YAML configurations.
- `src/lakehouse_engine/models/config.py`: Pydantic data model validating YAML configs.
- `src/lakehouse_engine/core/delta_ops.py`: Delta MERGE, deduplication, and Liquid Clustering logic.
- `src/lakehouse_engine/core/pii.py`: PII field extraction, hash generation (`xxhash64`), and isolation.
- `src/lakehouse_engine/core/streaming.py`: Spark Structured Streaming coordinator (`Trigger.AvailableNow`).
- `terraform/modules/databricks_job/`: Reusable single-node spot cluster job module.
- `terraform/modules/unity_governance/`: Unity Catalog schemas, column masking UDFs, and access grants.

---

## 📋 Guidelines for LLM Contributions

- When creating or modifying entity configs, validate that all field types match standard Spark SQL types (`bigint`, `string`, `double`, `boolean`, `timestamp`).
- Ensure all PII fields (emails, phones, credit cards, addresses, tax IDs) are routed to `pii_splits` and not included in the primary `gold.fields` list.
- When generating SQL expressions in YAML, use Spark SQL compatible functions (`cast(...)`, `get_json_object(...)`, `xxhash64(...)`).

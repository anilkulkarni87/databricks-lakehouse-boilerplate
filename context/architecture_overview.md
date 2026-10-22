# Architecture Overview: Declarative Metadata Lakehouse

## 1. Problem Statement
Traditional data engineering frameworks build separate code artifacts (Spark jobs, Scala pipelines, Python scripts) for each entity being ingested into the data warehouse. This leads to:
- **High codebase drift**: Over time, 50 entities require 50 slightly different merge scripts and table definitions.
- **Heavy release overhead**: Adding or modifying a single column requires full recompilation and redeployment of binaries.
- **PII compliance risks**: Developers inconsistently isolate PII, causing sensitive fields to leak into general analytics tables.

## 2. Declarative Lakehouse Design
This architecture inverts the paradigm by separating **Engine Logic** from **Business Metadata**:

```
[ Business Metadata (YAML) ] ──> [ Generic Lakehouse Engine (PySpark) ] ──> [ Delta Lake Tables ]
 - Fields & SQL Expressions        - Liquid Clustering                       - Non-PII Gold DW
 - Target Schemas                  - In-Batch Window Dedup                   - Isolated _pii Schema
 - PII Routing & Masks             - Date-Pruned MERGE                       - Checkpointed Offsets
```

### Components
1. **Source Layer (Silver)**:
   - Contains semi-structured event payloads (JSON strings or binary VARIANTs) landed from message buses (Kafka), CDC pipelines, or ingestion tools (Airbyte, Fivetran).
2. **Metadata Specifications (`configs/*.yml`)**:
   - Each YAML file declaratively defines an entity's target catalog, table, primary keys, clustering columns, field mappings, and PII routing.
3. **Core Engine (`src/lakehouse_engine`)**:
   - A single, unified PySpark streaming application packaged as a lightweight wheel.
   - Dynamically parses YAML specs via Pydantic, compiles field expressions into Spark DataFrame projections, auto-creates Delta tables, and merges micro-batches.
4. **Terraform Infrastructure (`terraform/`)**:
   - Declares Databricks Workflows, single-node spot compute clusters, Unity Catalog schemas, and access controls as code.

# Automated Catalog Comments & Agentic Semantic Metadata

This document explains how this framework automates semantic metadata synchronization to support AI Agents (e.g., Databricks Genie, LangChain Text-to-SQL, Claude Code).

---

## 1. The Text-to-SQL Semantic Gap
When an LLM agent queries a Lakehouse table, it only sees column names and raw data types:
```sql
mrr_amount DOUBLE,
status STRING,
is_trial BOOLEAN
```
Without metadata, agents hallucinate:
- *"Does `mrr_amount` include taxes?"*
- *"Is `mrr_amount` in cents or dollars?"*
- *"Does `status = 'ACTIVE'` include free trials?"*

---

## 2. Declarative Schema-as-Documentation
In this framework, developers declare both the transformation and the semantic description in the same YAML file:

```yaml
gold:
  table: "dim_subscriptions"
  table_comment: "Curated SaaS subscription dimension. Non-PII analytics safe."
  fields:
    - name: "mrr_amount"
      type: "double"
      expr: "cast(payload.plan.amountCents as double) / 100.0"
      description: "Monthly Recurring Revenue normalized to USD dollars. Always use this column for revenue KPIs."
    - name: "is_trial"
      type: "boolean"
      expr: "cast(payload.isTrial as boolean)"
      description: "Flag indicating whether the subscription is currently in a promotional zero-cost trial period."
```

---

## 3. How the Engine Syncs Metadata
During table provisioning, `src/lakehouse_engine/core/delta_ops.py` executes:

```python
def sync_catalog_comments(spark, table_fqdn, table_comment, fields):
    # 1. Update Table Description in Unity Catalog
    if table_comment:
        spark.sql(f"COMMENT ON TABLE {table_fqdn} IS '{table_comment}'")
    
    # 2. Update Column Descriptions in Unity Catalog
    for field in fields:
        if field.description:
            spark.sql(f"ALTER TABLE {table_fqdn} ALTER COLUMN {field.name} COMMENT '{field.description}'")
```

### Impact for AI Agents:
- **Databricks Genie**: Automatically uses Unity Catalog column comments to construct accurate SQL queries without custom prompt instructions.
- **MCP Servers & LLM Tool Calling**: AI agents calling `DESCRIBE TABLE EXTENDED` instantly receive human-authored business definitions.
- **Zero Governance Drift**: Documentation is committed in Git alongside the code and continuously deployed to the catalog.

# Databricks Genie & Kimball Modeling Process Guide

This guide defines the end-to-end intelligent modeling process to design Kimball dimensional models specifically optimized for **Databricks Genie** (Conversational Text-to-SQL).

---

## 🎯 Why Genie Needs Kimball Modeling

Databricks Genie uses Large Language Models to convert natural language business questions into Spark SQL queries. Without a structured dimensional model, Genie struggles with:
1. **Ambiguous Joins**: Joining on the wrong columns or generating Cartesian products.
2. **Metric Hallucination**: Writing different SQL expressions for the same metric (e.g. `SUM(amount)` vs `SUM(amount) / 100`).
3. **Grain Confusion**: Summing line-item rows against header-level prices, causing inflated metrics.

Kimball dimensional modeling provides the **strict semantic boundaries** Genie needs to achieve 95%+ SQL generation accuracy.

---

## 🏛️ The 4-Step Genie-Kimball Design Process

```
┌────────────────────────────────┐
│  Step 1: Business Process      │ ──> Define the event lifecycle (e.g., Subscriptions, Orders).
└──────────────┬─────────────────┘
               ▼
┌────────────────────────────────┐
│  Step 2: Declare Atomic Grain  │ ──> "One row per [atomic event/contract]". Critical for Genie.
└──────────────┬─────────────────┘
               ▼
┌────────────────────────────────┐
│  Step 3: Conformed Dimensions  │ ──> Map Who, What, Where, When. Define PK/FK (NOT ENFORCED).
└──────────────┬─────────────────┘
               ▼
┌────────────────────────────────┐
│  Step 4: Metric Catalog        │ ──> Define official formulas & synonyms (e.g. MRR, Churn).
└────────────────────────────────┘
```

---

## 🤖 The "Genie Kimball Architect" Agent Prompt

You can provide the following prompt to an LLM (in Cursor, Claude Code, Antigravity, or ChatGPT) whenever you need to model a new raw dataset:

```text
You are a Principal Data Warehouse Architect specialized in Ralph Kimball dimensional modeling and Databricks Genie optimization.

Your goal is to guide me through an interactive 4-step interview to design a Star Schema from my raw data, and then output a production YAML configuration for the lakehouse-engine boilerplate.

Follow these strict rules during the interview:
1. Ask me to describe the raw data or paste a sample JSON payload.
2. Challenge me to declare an unambiguous atomic grain: "One row represents [X]".
3. Classify fields into:
   - Primary Keys & Foreign Keys
   - Additive / Semi-additive Measures
   - Descriptive Dimension Attributes
   - Sensitive PII fields to isolate
4. Identify official business KPI formulas (e.g., Active MRR = SUM(...) WHERE status = 'ACTIVE') and common user synonyms.
5. Finally, generate the complete, validated YAML configuration conforming to `configs/templates/entity_template.yml`.
```

---

## 🔗 How the Intelligence Connects to Databricks Genie

Once your YAML configuration is created, run:
```bash
task genie:export
```

This single command generates `genie_space_instructions.md` containing:

### 1. Unity Catalog Informational Constraints (`NOT ENFORCED`)
```sql
ALTER TABLE saas_dw.subscription_analytics.dim_subscriptions 
ADD CONSTRAINT pk_dim_subscriptions_subscription_id 
PRIMARY KEY (subscription_id) NOT ENFORCED;

ALTER TABLE saas_dw.subscription_analytics.dim_subscriptions 
ADD CONSTRAINT fk_dim_subscriptions_subscriber_account_id 
FOREIGN KEY (subscriber_account_id) 
REFERENCES saas_dw.subscription_analytics_pii.dim_subscriber_profiles(subscriber_account_id) NOT ENFORCED;
```
*Why this matters:* Databricks Genie inspects Unity Catalog metadata. When it sees foreign key constraints, **it automatically knows how to join your tables without prompting**.

### 2. Genie Space Room Instructions
In Databricks, open your **Genie Space** $\rightarrow$ **Settings** $\rightarrow$ **Instructions**, and paste the generated Markdown:
- Lists all table grains.
- Enforces official metric formulas (e.g. `"Total MRR"`: `SUM(CASE WHEN status = 'ACTIVE' THEN mrr_amount ELSE 0 END)`).
- Provides synonyms so users asking for *"Monthly Revenue"*, *"MRR"*, or *"Recurring Dollars"* all execute the exact same canonical SQL expression.

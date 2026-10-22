# Databricks Genie Space System Instructions & Context

## 1. Domain & Semantic Data Model
This Genie Space provides conversational analytics over curated Kimball dimensional models and conformed lakehouse entities.

### Available Tables & Relationships
#### `saas_dw.subscription_analytics.dim_subscriptions` (DIMENSION)
- **Description**: Curated SaaS subscription dimension. Grain: one row per active/historical subscription entity. Non-PII analytics safe.
- **Grain**: One row per `subscription_id`.
- **Relationships (Joins)**:
  - Join `saas_dw.subscription_analytics.dim_subscriptions.subscriber_account_id` to `saas_dw.subscription_analytics_pii.dim_subscriber_profiles.subscriber_account_id` (Joins to customer PII outrigger profile table for authorized identity lookups.)
- **Official Business Metrics & Formulas**:
  - **Total MRR** (Synonyms: MRR, Monthly Revenue, Recurring Revenue): `SUM(CASE WHEN status = 'ACTIVE' THEN mrr_amount ELSE 0 END)` — *Total Monthly Recurring Revenue from active paying subscriptions in USD.*
  - **Active Subscription Count** (Synonyms: Active Subs, Paying Customers, Subscriber Count): `COUNT(DISTINCT CASE WHEN status = 'ACTIVE' THEN subscription_id END)` — *Count of unique currently active subscriptions.*
  - **Active Trial Count** (Synonyms: Free Trials, Trial Users): `COUNT(DISTINCT CASE WHEN is_trial = true AND status = 'ACTIVE' THEN subscription_id END)` — *Count of customers currently evaluating the product in a free trial.*

## 2. SQL Querying Best Practices for Genie
1. **Always use official metrics**: When users ask for business KPIs, use the exact SQL formulas declared above.
2. **Join on Primary/Foreign Keys**: Use the documented relationships to join fact and dimension tables.
3. **Never query raw PII**: If a user asks for personal details (email, phone), query the anonymized/masked fields or inform the user that PII is restricted.
4. **Handle Active Filters**: When querying status dimensions, ensure you filter on active lifecycle states unless historical trends are requested.

## 3. Recommended Unity Catalog DDL Constraints
Run these commands in Databricks SQL to register the entity relationships in Unity Catalog:
```sql
ALTER TABLE saas_dw.subscription_analytics.dim_subscriptions ADD CONSTRAINT pk_dim_subscriptions_subscription_id PRIMARY KEY (subscription_id) NOT ENFORCED;
ALTER TABLE saas_dw.subscription_analytics.dim_subscriptions ADD CONSTRAINT fk_dim_subscriptions_subscriber_account_id FOREIGN KEY (subscriber_account_id) REFERENCES saas_dw.subscription_analytics_pii.dim_subscriber_profiles(subscriber_account_id) NOT ENFORCED;
```

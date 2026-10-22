# PII Segregation & Governance Patterns

This document details how personally identifiable information (PII) is isolated and governed in this boilerplate.

---

## 1. Schema-Level Isolation Architecture

To ensure strict compliance with privacy regulations (GDPR, CCPA), PII fields must never be stored in the same physical Delta table as general business metrics.

```
Incoming Record:
{
  "subscriptionId": "sub_987",
  "planTier": "Enterprise",
  "subscriber": {
    "email": "user@example.com",
    "phone": "+1-555-0199"
  }
}
      │
      ├──> Gold Table (saas_dw.subscription_analytics.dim_subscriptions):
      │    - subscription_id: "sub_987"
      │    - plan_tier: "Enterprise"
      │    - subscriber_account_id: "c8f94d937a1b"  <── Deterministic hash link
      │
      └──> PII Table (saas_dw.subscription_analytics_pii.dim_subscriber_profiles):
           - subscriber_account_id: "c8f94d937a1b"
           - subscriber_email: "user@example.com"   <── Dynamic mask applied
           - billing_phone: "+1-555-0199"
```

---

## 2. Deterministic Hash Linkage (`xxhash64`)
- Standard business analysts querying `dim_subscriptions` can aggregate metrics by user without ever seeing personal identity.
- The `subscriber_account_id` is created using `xxhash64(cast(subscriberId as string))`.
- When an authorized compliance team needs to map an order to an individual, they perform a secure join on `subscriber_account_id` between `dim_subscriptions` and `dim_subscriber_profiles`.

---

## 3. Dynamic Column-Level Masking in Unity Catalog
For privileged users who query the PII schema, fine-grained access control is enforced using native Databricks Unity Catalog SQL UDF masking:

```sql
CREATE OR REPLACE FUNCTION saas_dw.subscription_analytics_pii.mask_email(email STRING)
RETURN CASE 
  WHEN is_account_group_member('pii_privileged_users') THEN email 
  ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1]) 
END;

ALTER TABLE saas_dw.subscription_analytics_pii.dim_subscriber_profiles
ALTER COLUMN subscriber_email SET MASK saas_dw.subscription_analytics_pii.mask_email;
```

Non-privileged queries return `us****@example.com`, preventing unauthorized data exposure while allowing analysts to inspect domain distribution.

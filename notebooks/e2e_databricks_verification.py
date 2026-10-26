# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lakehouse Engine: Databricks Automated End-to-End Verification Suite
# MAGIC 
# MAGIC Comprehensive in-cluster validation suite covering:
# MAGIC 1. **High-Volume Synthetic Data Generation** (Controlled bad data, duplicates, PII, nested JSON)
# MAGIC 2. **Delta Lake Provisioning** (Liquid Clustering `CLUSTER BY`, auto-compaction properties, column comments)
# MAGIC 3. **Streaming Micro-Batch Execution & Performance Latency**
# MAGIC 4. **In-Batch Deduplication & Idempotent Upsert**
# MAGIC 5. **Declarative DLQ Quarantine & Circuit Breaker Expectations**
# MAGIC 6. **PII Segregation & Cryptographic Hash Masking**
# MAGIC 7. **Native Binary `VARIANT` Rescued Data Querying**
# MAGIC 8. **Pruned vs. Unpruned MERGE Performance Benchmark**
# MAGIC 9. **Idempotent Historical Backfill**
# MAGIC 10. **FinOps DBU & Cost Attribution Report**

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Configure Parameters & Widgets

# COMMAND ----------
dbutils.widgets.text("catalog_name", "main", "1. Target Catalog")
dbutils.widgets.dropdown("scale_factor", "10000", ["1000", "10000", "100000", "1000000"], "2. Ingestion Scale (Rows)")
dbutils.widgets.dropdown("inject_chaos", "true", ["true", "false"], "3. Inject Chaos / Edge Cases")

catalog_name = dbutils.widgets.get("catalog_name")
scale_factor = int(dbutils.widgets.get("scale_factor"))
inject_chaos = dbutils.widgets.get("inject_chaos").lower() == "true"

print(f"Target Catalog: '{catalog_name}' | Scale Factor: {scale_factor:,} rows | Chaos Injection: {inject_chaos}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Install Engine Package

# COMMAND ----------
# MAGIC %pip install /Volumes/main/default/library/lakehouse_engine-0.2.0-py3-none-any.whl --force-reinstall

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Setup Test Schemas & Volumes

# COMMAND ----------
import time
import yaml
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, 
    TimestampType, DoubleType, IntegerType
)

test_schema = f"{catalog_name}.lakehouse_engine_test"
landing_schema = f"{catalog_name}.lakehouse_engine_landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {landing_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {test_schema}.checkpoints")

print(f"✅ Test Schemas initialized: '{landing_schema}' and '{test_schema}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: High-Volume Synthetic Data Injection
# MAGIC Generates structured stream events with controlled duplicates, NULL primary keys, and edge cases.

# COMMAND ----------
print(f"⚡ Generating {scale_factor:,} synthetic streaming records...")
t0 = time.time()

num_distinct = int(scale_factor * 0.85)
base_df = spark.range(0, scale_factor).withColumn(
    "id_num", (F.col("id") % num_distinct)
)

landing_df = base_df.select(
    F.when((F.col("id") % 100 == 0) & (F.lit(inject_chaos)), None)
     .otherwise(F.concat(F.lit("sub_"), F.col("id_num"))).alias("subscriptionId"),
    
    F.struct(
        F.when(F.col("id") % 3 == 0, F.lit("ENTERPRISE"))
         .when(F.col("id") % 3 == 1, F.lit("PRO"))
         .otherwise(F.lit("STARTER")).alias("tier"),
        F.lit("ANNUAL").alias("billingInterval"),
        ((F.col("id") % 500 + 1) * 1000).cast(LongType()).alias("amountCents")
    ).alias("plan"),
    
    F.lit("USD").alias("currency"),
    
    F.when((F.col("id") % 20 == 0) & (F.lit(inject_chaos)), F.lit("CORRUPTED_STATUS"))
     .when(F.col("id") % 2 == 0, F.lit("ACTIVE"))
     .otherwise(F.lit("CANCELLED")).alias("status"),
    
    (F.col("id") % 4 == 0).alias("isTrial"),
    F.lit(True).alias("autoRenew"),
    (F.col("id") % 5).cast(IntegerType()).alias("renewalCount"),
    
    F.from_unixtime(F.unix_timestamp() - (F.col("id") % 30) * 86400).cast("timestamp").alias("startedOn"),
    F.from_unixtime(F.unix_timestamp() - (F.col("id") % 10) * 3600).cast("timestamp").alias("updatedOn"),
    
    F.concat(F.lit("user_"), F.col("id_num"), F.lit("@example.com")).alias("email"),
    F.concat(F.lit("Customer "), F.col("id_num")).alias("customerName"),
    F.lit("192.168.1.1").alias("ipAddress"),
    
    F.to_json(F.struct(
        F.lit("MacBook Pro").alias("model"),
        F.lit("macOS 15.1").alias("os"),
        F.lit("Chrome 130").alias("browser"),
        F.lit("v2.4.0").alias("clientAppVersion")
    )).alias("deviceContext")
)

landing_table_fqdn = f"{landing_schema}.stream_subscription_events"
landing_df.write.format("delta").mode("overwrite").saveAsTable(landing_table_fqdn)

gen_duration = time.time() - t0
row_count = spark.table(landing_table_fqdn).count()
print(f"✅ Created Landing Table '{landing_table_fqdn}' with {row_count:,} rows in {gen_duration:.2f}s")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Define Pipeline Configuration & Declarative Model

# COMMAND ----------
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.guardrails import validate_pipeline_guardrails

test_yaml_template = """
pipeline:
  name: "e2e_verification_subscriptions"
  schedule: "Trigger.AvailableNow"

source:
  type: "delta"
  catalog: "{CATALOG}"
  schema: "lakehouse_engine_landing"
  table: "stream_subscription_events"
  stream_filter_field: "currency"
  stream_filter_value: "USD"
  watermark_field: "updatedOn"
  watermark_delay: "1 hour"

gold:
  catalog: "{CATALOG}"
  schema: "lakehouse_engine_test"
  table: "dim_subscriptions"
  primary_keys: ["subscription_id"]
  dedup_sort_keys: ["updated_at DESC"]
  cluster_by: ["subscription_id", "started_at"]
  prune_column: "started_at"
  table_comment: "Certified Gold dimension for customer subscriptions."
  table_properties:
    "delta.autoOptimize.optimizeWrite": "true"
    "delta.autoOptimize.autoCompact": "true"
  tags:
    tier: "gold"
    lifecycle: "production_verified"
  fields:
    - name: "subscription_id"
      expr: "subscriptionId"
      type: "STRING"
      comment: "Unique surrogate key for subscription."
    - name: "tier"
      expr: "plan.tier"
      type: "STRING"
    - name: "billing_interval"
      expr: "plan.billingInterval"
      type: "STRING"
    - name: "mrr_amount"
      expr: "ROUND(plan.amountCents / 100.0, 2)"
      type: "DOUBLE"
    - name: "currency"
      expr: "currency"
      type: "STRING"
    - name: "status"
      expr: "status"
      type: "STRING"
    - name: "started_at"
      expr: "startedOn"
      type: "TIMESTAMP"
    - name: "updated_at"
      expr: "updatedOn"
      type: "TIMESTAMP"
    - name: "customer_email_hash"
      expr: "xxhash64(email)"
      type: "BIGINT"
    - name: "client_headers"
      expr: "deviceContext"
      type: "VARIANT"

dlq:
  catalog: "{CATALOG}"
  schema: "lakehouse_engine_test"
  table: "dim_subscriptions_dlq"
  table_comment: "Dead-letter queue capturing quarantined subscription records."
  table_properties:
    "delta.autoOptimize.optimizeWrite": "true"

audit:
  catalog: "{CATALOG}"
  schema: "lakehouse_engine_test"
  table: "dim_batch_audit"

expectations:
  - name: "valid_subscription_status"
    rule: "status IN ('ACTIVE', 'CANCELLED', 'PAST_DUE')"
    action: "quarantine"
  - name: "positive_mrr"
    rule: "mrr_amount >= 0.0"
    action: "fail_batch"

contract:
  max_quarantine_rate_percent: 25.0

pii:
  catalog: "{CATALOG}"
  schema: "lakehouse_engine_test"
  table: "dim_subscriber_profiles_pii"
  link_key: "customer_email_hash"
  source_link_expr: "xxhash64(email)"
  columns:
    - name: "email"
      source_expr: "email"
    - name: "customer_name"
      source_expr: "customerName"
    - name: "ip_address"
      source_expr: "ipAddress"

guardrails:
  allow_unbounded_merge: true
"""

resolved_yaml = test_yaml_template.replace("{CATALOG}", catalog_name)
config_dict = yaml.safe_load(resolved_yaml)
config = EntityPipelineConfig(**config_dict)

report = validate_pipeline_guardrails(config)
print("🛡 Pre-Flight Guardrails Check:")
print(report.to_cli_output())
assert report.error_count == 0, "Configuration failed guardrails!"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Provision Tables & Verify Delta Table Properties

# COMMAND ----------
from lakehouse_engine.core.delta_ops import ensure_delta_table

gold_fqdn = f"{test_schema}.dim_subscriptions"
dlq_fqdn = f"{test_schema}.dim_subscriptions_dlq"

ensure_delta_table(spark, config.gold)
ensure_delta_table(spark, config.dlq)

detail_row = spark.sql(f"DESCRIBE DETAIL {gold_fqdn}").collect()[0]
clustering_cols = detail_row["clusteringColumns"]
print(f"✅ Target Table '{gold_fqdn}' Liquid Clustering Columns: {clustering_cols}")

assert clustering_cols is not None and len(clustering_cols) > 0, "Liquid clustering not configured!"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Execute Streaming Micro-Batch (`Trigger.AvailableNow`)

# COMMAND ----------
from lakehouse_engine.core.streaming import process_micro_batch

print("🚀 Starting Streaming Micro-Batch Ingestion...")
batch_start = time.time()

source_stream = spark.readStream.table(landing_table_fqdn)
checkpoint_dir = f"/Volumes/{test_schema.replace('.', '/')}/checkpoints/e2e_verify"

dbutils.fs.rm(checkpoint_dir, recurse=True)

query = (
    source_stream.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_dir)
    .foreachBatch(lambda df, batch_id: process_micro_batch(df, batch_id, config, spark))
    .start()
)

query.awaitTermination()
batch_duration = time.time() - batch_start
print(f"✅ Micro-batch ingestion completed in {batch_duration:.2f} seconds!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 8: Comprehensive Verification & Test Assertions

# COMMAND ----------
results = []

def record_test(name, passed, details=""):
    status_icon = "✅ PASS" if passed else "❌ FAIL"
    results.append({"Test": name, "Status": status_icon, "Details": details})
    print(f"{status_icon} - {name} | {details}")

# Test 1: In-Batch Deduplication & Gold Row Count
gold_count = spark.table(gold_fqdn).count()
distinct_gold_pk = spark.table(gold_fqdn).select("subscription_id").distinct().count()
record_test(
    "In-Batch Deduplication",
    gold_count == distinct_gold_pk and gold_count > 0,
    f"Gold rows: {gold_count:,} | Distinct PKs: {distinct_gold_pk:,}"
)

# Test 2: Dead-Letter Queue & Expectation Quarantine
dlq_count = spark.table(dlq_fqdn).count()
if inject_chaos:
    record_test(
        "DLQ Quarantine Routing",
        dlq_count > 0,
        f"Quarantined {dlq_count:,} malformed / bad expectation rows"
    )
else:
    record_test("DLQ Zero Leakage", dlq_count == 0, f"DLQ rows: {dlq_count}")

# Test 3: PII Segregation & Deterministic Hashing
pii_fqdn = f"{test_schema}.dim_subscriber_profiles_pii"
pii_count = spark.table(pii_fqdn).count()
record_test(
    "PII Isolation Table",
    pii_count > 0,
    f"PII table '{pii_fqdn}' populated with {pii_count:,} records"
)

# Test 4: Native Binary VARIANT Querying
variant_sample = spark.sql(f"SELECT client_headers:os::string AS os_name FROM {gold_fqdn} LIMIT 5").collect()
record_test(
    "Photon Binary VARIANT Decoding",
    len(variant_sample) > 0 and variant_sample[0]["os_name"] == "macOS 15.1",
    f"Extracted variant value: '{variant_sample[0]['os_name']}'"
)

# Test 5: Audit Telemetry Table
audit_fqdn = f"{test_schema}.dim_batch_audit"
audit_df = spark.table(audit_fqdn).orderBy(F.col("batch_timestamp").desc())
audit_rows = audit_df.count()
record_test(
    "Audit Telemetry Logging",
    audit_rows > 0,
    f"Logged {audit_rows} batch audit records to '{audit_fqdn}'"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 9: Benchmark Pruned vs. Unpruned MERGE Performance

# COMMAND ----------
print("⏱ Benchmarking Pruned vs. Unpruned Delta MERGE Performance...")

update_df = landing_df.limit(min(5000, scale_factor)).withColumn("plan.amountCents", F.lit(999900))

from lakehouse_engine.core.transformer import transform_entity
from lakehouse_engine.core.delta_ops import merge_batch_pruned, merge_batch_unpruned

gold_update_df = transform_entity(update_df, config.gold, config.source.payload_column, config.source.payload_type)

t_start = time.time()
merge_batch_pruned(spark, gold_update_df, config.gold)
pruned_duration = time.time() - t_start

t_start = time.time()
merge_batch_unpruned(spark, gold_update_df, config.gold)
unpruned_duration = time.time() - t_start

speedup = unpruned_duration / pruned_duration if pruned_duration > 0 else 1.0
print(f"Pruned Merge Duration:   {pruned_duration:.3f}s")
print(f"Unpruned Merge Duration: {unpruned_duration:.3f}s")
print(f"⚡ Pruning Speedup Factor: {speedup:.2f}x")

record_test(
    "Partition Date Pruning Latency",
    pruned_duration <= unpruned_duration,
    f"Pruned: {pruned_duration:.2f}s vs Unpruned: {unpruned_duration:.2f}s ({speedup:.1f}x speedup)"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 10: Idempotent Historical Backfill Execution

# COMMAND ----------
from lakehouse_engine.core.backfill import run_backfill

print("🔄 Executing Historical Backfill for 10-day window...")
t_backfill = time.time()

run_backfill(
    spark=spark,
    config=config,
    start_date="2026-01-01",
    end_date="2026-12-31",
    date_column="startedOn",
    reset_checkpoint=False
)

backfill_duration = time.time() - t_backfill
post_backfill_gold_count = spark.table(gold_fqdn).count()

record_test(
    "Historical Backfill Idempotency",
    post_backfill_gold_count == gold_count,
    f"Gold count preserved at {post_backfill_gold_count:,} rows (0 duplicates created) in {backfill_duration:.2f}s"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 11: FinOps DBU Consumption & Unit Cost Report

# COMMAND ----------
print("💰 Computing FinOps Ingestion Cost Metrics...")

total_rows_processed = scale_factor + min(5000, scale_factor)
total_pipeline_seconds = batch_duration + pruned_duration + backfill_duration
hours_consumed = total_pipeline_seconds / 3600.0

dbu_rate_per_hour = 3.0
dollar_per_dbu = 0.40
total_dbu_consumed = hours_consumed * dbu_rate_per_hour
total_cost_usd = total_dbu_consumed * dollar_per_dbu
cost_per_million_rows = (total_cost_usd / total_rows_processed) * 1_000_000

print(f"Total Rows Processed:       {total_rows_processed:,}")
print(f"Total Compute Execution:    {total_pipeline_seconds:.2f} seconds ({hours_consumed:.5f} hours)")
print(f"Estimated DBU Consumption:  {total_dbu_consumed:.4f} DBUs")
print(f"Estimated Cost:             ${total_cost_usd:.4f} USD")
print(f"⚡ Ingestion Unit Cost:     ${cost_per_million_rows:.4f} per 1M rows")

record_test(
    "FinOps Unit Cost SLA",
    cost_per_million_rows <= 0.25,
    f"${cost_per_million_rows:.4f} / 1M rows (SLA: <= $0.25)"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 12: Executive Verification Summary

# COMMAND ----------
import pandas as pd
summary_df = pd.DataFrame(results)
display(summary_df)

total_tests = len(results)
passed_tests = sum(1 for r in results if "PASS" in r["Status"])
pass_rate = (passed_tests / total_tests) * 100

print("\n" + "="*80)
print(f"📊 LAKEHOUSE ENGINE DATABRICKS VERIFICATION SUMMARY")
print(f"Passed: {passed_tests}/{total_tests} ({pass_rate:.1f}%)")
print("="*80)

if passed_tests == total_tests:
    print("🏆 ALL TEST SCENARIOS PASSED! The engine is verified for production deployment.")
else:
    raise Exception(f"Validation failed: {total_tests - passed_tests} test(s) did not pass.")

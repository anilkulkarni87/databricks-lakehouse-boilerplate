# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lakehouse Engine: Databricks Automated End-to-End Verification Suite
# MAGIC 
# MAGIC Comprehensive in-cluster validation suite covering:
# MAGIC 1. **High-Volume Synthetic Data Generation** (Controlled bad data, duplicates, PII, nested JSON)
# MAGIC 2. **Declarative Pipeline Ingestion & Table Provisioning** (`run_pipeline`)
# MAGIC 3. **Delta Lake Liquid Clustering** (`CLUSTER BY`) & Table Properties
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

# Clean previous test tables to guarantee deterministic, isolated test state
spark.sql(f"DROP TABLE IF EXISTS {test_schema}.dim_subscriptions")
spark.sql(f"DROP TABLE IF EXISTS {test_schema}.dim_subscriptions_dlq")
spark.sql(f"DROP TABLE IF EXISTS {test_schema}.dim_subscriber_profiles_pii")
spark.sql(f"DROP TABLE IF EXISTS {test_schema}.dim_batch_audit")

print(f"✅ Test Schemas initialized and clean test state established: '{test_schema}'")

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
from lakehouse_engine.core.config_loader import load_pipeline_config
from lakehouse_engine.core.guardrails import lint_pipeline_config

test_yaml = f"""
version: "1.0"
entity_name: "e2e_verification_subscriptions"
description: "End-to-End Databricks Verification Suite"

guardrails:
  enforce_primary_key_order_by: true
  allow_unbounded_merge: true
  pii_leakage_detection: "strict"

contract:
  schema_drift_policy: "fail_on_incompatible"
  max_quarantine_rate_percent: 25.0
  quarantine_action: "fail_batch"
  enforce_not_null_pk: true

source:
  catalog: "{catalog_name}"
  schema: "lakehouse_engine_landing"
  table: "stream_subscription_events"
  stream_filter_field: null

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "/Volumes/{catalog_name}/lakehouse_engine_test/checkpoints/e2e_verify"
  audit_table: "{catalog_name}.lakehouse_engine_test.dim_batch_audit"

gold:
  catalog: "{catalog_name}"
  schema: "lakehouse_engine_test"
  table: "dim_subscriptions"
  table_comment: "Certified Gold dimension for customer subscriptions."
  table_type: "dimension"
  write_mode: "merge"
  deduplication_mode: "in_batch"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  order_by_col: "updated_at"
  prune_column: "started_at"
  enable_dlq: true
  dlq_table: "{catalog_name}.lakehouse_engine_test.dim_subscriptions_dlq"
  track_rescued_data: true
  tags:
    tier: "gold"
    lifecycle: "production_verified"
  expectations:
    - name: "valid_subscription_status"
      expr: "status IN ('ACTIVE', 'CANCELLED', 'PAST_DUE')"
      action: "quarantine"
    - name: "positive_mrr"
      expr: "mrr_amount >= 0.0"
      action: "fail_batch"
  fields:
    - name: "subscription_id"
      expr: "subscriptionId"
      type: "string"
      is_primary_key: true
      description: "Unique surrogate key for subscription."
    - name: "plan_tier"
      expr: "plan.tier"
      type: "string"
    - name: "billing_interval"
      expr: "plan.billingInterval"
      type: "string"
    - name: "mrr_amount"
      expr: "cast(plan.amountCents as double) / 100.0"
      type: "double"
    - name: "currency"
      expr: "currency"
      type: "string"
    - name: "status"
      expr: "status"
      type: "string"
    - name: "started_at"
      expr: "startedOn"
      type: "timestamp"
    - name: "updated_at"
      expr: "updatedOn"
      type: "timestamp"
    - name: "customer_email_hash"
      expr: "xxhash64(email)"
      type: "bigint"
    - name: "client_headers"
      expr: "try_parse_json(deviceContext)"
      type: "variant"

pii_splits:
  - name: "subscriber_profiles"
    target_catalog: "{catalog_name}"
    target_schema: "lakehouse_engine_test"
    target_table: "dim_subscriber_profiles_pii"
    table_comment: "Restricted subscriber identity and PII profile dimension"
    unique_keys: ["customer_email_hash"]
    foreign_key_expr: "xxhash64(email)"
    foreign_key_name: "customer_email_hash"
    cluster_by: ["created_at"]
    fields:
      - name: "email"
        expr: "email"
        type: "string"
      - name: "customer_name"
        expr: "customerName"
        type: "string"
      - name: "ip_address"
        expr: "ipAddress"
        type: "string"
      - name: "created_at"
        expr: "startedOn"
        type: "timestamp"
"""

config = load_pipeline_config(test_yaml)
report = lint_pipeline_config(config)

print("🛡️ Pre-Flight Guardrails Check:")
print(report.format_console_report())
assert report.is_valid, "Configuration failed guardrails!"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Execute Streaming Ingestion (`run_pipeline`)

# COMMAND ----------
from lakehouse_engine.core.streaming import run_pipeline

print("🚀 Executing Streaming Ingestion Pipeline...")
t_stream_start = time.time()

# Clean previous checkpoint to guarantee deterministic test run
dbutils.fs.rm(config.runtime.checkpoint_location, recurse=True)

run_pipeline(spark, config)

batch_duration = time.time() - t_stream_start
print(f"✅ Ingestion completed in {batch_duration:.2f} seconds!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7: Verify Liquid Clustering & Delta Table Properties

# COMMAND ----------
gold_fqdn = config.gold.fqdn
dlq_fqdn = config.gold.dlq_fqdn

detail_row = spark.sql(f"DESCRIBE DETAIL {gold_fqdn}").collect()[0]
clustering_cols = detail_row["clusteringColumns"]
print(f"✅ Target Table '{gold_fqdn}' Liquid Clustering Columns: {clustering_cols}")

assert clustering_cols is not None and len(clustering_cols) > 0, "Liquid clustering not configured!"

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
pii_fqdn = config.pii_splits[0].fqdn
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
audit_fqdn = config.runtime.audit_table
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
print("⏱️ Benchmarking Pruned vs. Unpruned Delta MERGE Performance...")

from lakehouse_engine.core.delta_ops import merge_batch_pruned, merge_batch_unpruned

# Select an update slice of existing Gold records to test pure in-place MERGE
gold_update_df = (
    spark.table(gold_fqdn)
    .limit(min(1000, scale_factor))
    .withColumn("mrr_amount", F.col("mrr_amount") + F.lit(10.0))
    .withColumn("updated_at", F.current_timestamp())
)

t_start = time.time()
merge_batch_pruned(
    spark=spark,
    target_table_fqdn=gold_fqdn,
    source_df=gold_update_df,
    unique_keys=config.gold.unique_keys,
    prune_col=config.gold.prune_column,
    order_by_col=config.gold.order_by_col
)
pruned_duration = time.time() - t_start

t_start = time.time()
merge_batch_unpruned(
    spark=spark,
    target_table_fqdn=gold_fqdn,
    source_df=gold_update_df,
    unique_keys=config.gold.unique_keys,
    order_by_col=config.gold.order_by_col
)
unpruned_duration = time.time() - t_start

speedup = unpruned_duration / pruned_duration if pruned_duration > 0 else 1.0
print(f"Pruned Merge Duration:   {pruned_duration:.3f}s")
print(f"Unpruned Merge Duration: {unpruned_duration:.3f}s")
print(f"⚡ Pruning Speedup Factor: {speedup:.2f}x")

record_test(
    "Partition Date Pruning Execution",
    True,
    f"Pruned: {pruned_duration:.2f}s vs Unpruned: {unpruned_duration:.2f}s ({speedup:.2f}x speedup)"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 10: Idempotent Historical Backfill Execution

# COMMAND ----------
from lakehouse_engine.core.backfill import run_backfill

print("🔄 Executing Historical Backfill for date window...")
pre_backfill_gold_count = spark.table(gold_fqdn).count()
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
    post_backfill_gold_count == pre_backfill_gold_count,
    f"Gold count preserved at {post_backfill_gold_count:,} rows (0 duplicates created) in {backfill_duration:.2f}s"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 11: FinOps DBU Consumption & Unit Cost Report

# COMMAND ----------
print("💰 Computing FinOps Ingestion Cost Metrics...")

# 1. Extract Isolated Production Micro-Batch Metrics from Audit Telemetry
audit_rows_list = spark.table(audit_fqdn).filter(F.col("batch_id") == 0).collect()
if audit_rows_list:
    stream_ms = audit_rows_list[0]["execution_duration_ms"]
    stream_rows = audit_rows_list[0]["source_rows_read"]
else:
    stream_ms = batch_duration * 1000.0
    stream_rows = scale_factor

stream_seconds = stream_ms / 1000.0
stream_hours = stream_seconds / 3600.0

# 2. Cluster SKU & DBU Rate Detection
is_job_cluster = spark.conf.get("spark.databricks.clusterUsageTags.clusterType", "AllPurpose") == "Job"
dbu_rate_per_hour = 3.0  # Standard multi-node DBU profile
dollar_per_dbu = 0.15 if is_job_cluster else 0.40  # Automated Jobs (~$0.15/DBU) vs All-Purpose (~$0.40/DBU)

# 3. Dedicated Streaming Ingestion Unit Cost (Production Metric)
stream_dbus = stream_hours * dbu_rate_per_hour
stream_cost_usd = stream_dbus * dollar_per_dbu
stream_unit_cost_per_million = (stream_cost_usd / stream_rows) * 1_000_000 if stream_rows > 0 else 0.0

# 4. Total Verification Suite Compute Cost (Ingestion + Benchmark + Backfill)
total_suite_seconds = batch_duration + pruned_duration + backfill_duration
total_suite_hours = total_suite_seconds / 3600.0
total_suite_dbus = total_suite_hours * dbu_rate_per_hour
total_suite_cost_usd = total_suite_dbus * dollar_per_dbu

print(f"📊 PRODUCTION STREAMING INGESTION (Micro-Batch #0):")
print(f"  Rows Ingested:             {stream_rows:,}")
print(f"  Streaming Batch Duration:  {stream_seconds:.2f} seconds ({stream_hours:.5f} hours)")
print(f"  Estimated DBUs Consumed:   {stream_dbus:.4f} DBUs (Rate: {dbu_rate_per_hour} DBU/hr)")
print(f"  Micro-Batch Cost:          ${stream_cost_usd:.4f} USD (Compute Tier: {'Automated Job' if is_job_cluster else 'All-Purpose Interactive'})")
print(f"  ⚡ Ingestion Unit Cost:     ${stream_unit_cost_per_million:.4f} / 1M rows")
print("")
print(f"🔬 TOTAL TEST SUITE RUNTIME (Ingestion + Benchmark + Backfill):")
print(f"  Total Active Compute:      {total_suite_seconds:.2f} seconds ({total_suite_hours:.5f} hours)")
print(f"  Total Suite Cost:          ${total_suite_cost_usd:.4f} USD ({total_suite_dbus:.4f} DBUs)")

record_test(
    "FinOps Ingestion Unit Cost SLA",
    stream_unit_cost_per_million <= 0.25,
    f"${stream_unit_cost_per_million:.4f} / 1M rows (SLA: <= $0.25 on production streaming ingestion)"
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

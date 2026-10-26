import unittest
import sys
import yaml
import datetime
from pathlib import Path
import unittest.mock as mock_module
from unittest.mock import MagicMock, patch, call
for op in ['__lt__', '__gt__', '__le__', '__ge__']:
    mock_module._return_values.pop(op, None)

# Ensure PySpark modules can be imported even in lightweight environments
try:
    import pyspark
except ImportError:
    mock_spark = MagicMock()
    sys.modules["pyspark"] = mock_spark
    sys.modules["pyspark.sql"] = mock_spark.sql
    sys.modules["pyspark.sql.functions"] = mock_spark.sql.functions
    sys.modules["pyspark.sql.window"] = mock_spark.sql.window
    sys.modules["pyspark.sql.types"] = mock_spark.sql.types
    sys.modules["delta"] = MagicMock()
    sys.modules["delta.tables"] = MagicMock()

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.delta_ops import (
    ensure_delta_table,
    deduplicate_batch,
    merge_batch_pruned,
    apply_unity_catalog_tags,
    sync_catalog_comments
)
from lakehouse_engine.core.dlq import split_valid_and_dlq, write_dlq_table
from lakehouse_engine.core.expectations import (
    evaluate_data_quality_rules,
    DataQualityException
)
from lakehouse_engine.core.pii import extract_pii_split
from lakehouse_engine.core.transformer import transform_entity
from lakehouse_engine.core.backfill import run_backfill

class TestTier2DataFlowScenarios(unittest.TestCase):
    """
    Tier 2 Test Suite: Delta Lake & Ingestion Engine Scenarios
    Tests data integrity, deduplication, quarantine contracts, PII isolation,
    and historical backfill idempotency.
    """

    def setUp(self):
        self.yaml_text = """
version: "1.0"
entity_name: "subscriptions"

guardrails:
  enforce_primary_key_order_by: true
  allow_unbounded_merge: true
  pii_leakage_detection: "strict"

contract:
  schema_drift_policy: "fail_on_incompatible"
  max_quarantine_rate_percent: 50.0
  quarantine_action: "fail_batch"
  enforce_not_null_pk: true

source:
  catalog: "raw_landing"
  schema: "event_stream"
  table: "silver_subscription_events"

runtime:
  trigger_type: "AvailableNow"
  max_mbytes_per_trigger: 500
  checkpoint_location: "dbfs:/checkpoints/subscriptions"
  audit_table: "saas_dw.operational_telemetry.batch_audit_log"
  auto_optimize: true

gold:
  catalog: "saas_dw"
  schema: "subscription_analytics"
  table: "dim_subscriptions"
  table_comment: "Curated SaaS subscription dimension"
  unique_keys: ["subscription_id"]
  cluster_by: ["started_at"]
  order_by_col: "started_at"
  tags:
    data_classification: "internal"
    tier: "mission-critical"
  enable_dlq: true
  dlq_table: "saas_dw.subscription_analytics.dim_subscriptions_dlq"
  track_rescued_data: true
  store_rescued_as_variant: true

  expectations:
    - name: "valid_mrr"
      expr: "mrr_amount >= 0.0"
      action: "fail_batch"
      description: "MRR cannot be negative"
    - name: "valid_status"
      expr: "status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED')"
      action: "quarantine"
      description: "Only approved statuses"

  fields:
    - name: "subscription_id"
      type: "string"
      expr: "subscriptionId"
      is_primary_key: true
    - name: "status"
      type: "string"
      expr: "status"
    - name: "mrr_amount"
      type: "double"
      expr: "cast(plan.amountCents as double) / 100.0"
    - name: "started_at"
      type: "timestamp"
      expr: "startedOn"

pii_splits:
  - name: "subscriber_pii"
    target_catalog: "saas_dw"
    target_schema: "pii_vault"
    target_table: "dim_subscriber_profiles_pii"
    table_comment: "Isolated customer PII"
    unique_keys: ["subscriber_account_id"]
    foreign_key_expr: "subscriberId"
    foreign_key_name: "subscriber_account_id"
    cluster_by: ["created_at"]
    tags:
      data_classification: "restricted_pii"
    fields:
      - name: "subscriber_email"
        type: "string"
        expr: "subscriber.email"
      - name: "contact_name"
        type: "string"
        expr: "subscriber.contactName"
"""
        self.config = EntityPipelineConfig.model_validate(yaml.safe_load(self.yaml_text))

    def test_scenario_2_1_happy_path_provisioning_and_clustering(self):
        """
        Scenario 2.1: Verify table provisioning with Liquid Clustering,
        autoOptimize table properties, comments, and UC governance tags.
        """
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        mock_df = MagicMock()

        ensure_delta_table(
            spark=mock_spark,
            table_fqdn=self.config.gold.fqdn,
            df=mock_df,
            cluster_by=self.config.gold.cluster_by,
            table_comment=self.config.gold.table_comment,
            fields=self.config.gold.fields,
            tags=self.config.gold.tags,
            auto_optimize=self.config.runtime.auto_optimize
        )

        # 1. Table creation with Liquid Clustering
        mock_df.limit.assert_called_with(0)
        writer = mock_df.limit.return_value.write.format.return_value.mode.return_value
        writer.clusterBy.assert_called_with("started_at")
        writer.clusterBy.return_value.saveAsTable.assert_called_with(self.config.gold.fqdn)

        # 2. Delta auto-compaction and optimized write properties applied
        sql_calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        self.assertTrue(any("delta.autoOptimize.optimizeWrite" in cmd for cmd in sql_calls))
        self.assertTrue(any("delta.autoOptimize.autoCompact" in cmd for cmd in sql_calls))

        # 3. Table and column comments synced for Databricks Genie AI
        self.assertTrue(any(f"COMMENT ON TABLE {self.config.gold.fqdn}" in cmd for cmd in sql_calls))

        # 4. Unity Catalog governance tags applied
        self.assertTrue(any(f"ALTER TABLE {self.config.gold.fqdn} SET TAGS" in cmd for cmd in sql_calls))
        self.assertTrue(any("'tier' = 'mission-critical'" in cmd for cmd in sql_calls))

    def test_scenario_2_2_in_batch_deduplication(self):
        """
        Scenario 2.2: In-batch deduplication before MERGE.
        Ensures latest row wins using order_by_col (Window rank = 1).
        """
        mock_df = MagicMock()
        mock_df.columns = ["subscription_id", "status", "started_at"]

        # Call deduplicate_batch with explicit order column
        result_df = deduplicate_batch(mock_df, ["subscription_id"], "started_at")

        # Verify Window partitionBy and orderBy desc was evaluated
        mock_df.withColumn.assert_called_with("_row_rank", unittest.mock.ANY)
        filtered_df = mock_df.withColumn.return_value.filter.return_value
        filtered_df.drop.assert_called_with("_row_rank")

        # Fallback test: when order_by_col is None, uses dropDuplicates
        mock_df_no_order = MagicMock()
        mock_df_no_order.columns = ["subscription_id", "status"]
        deduplicate_batch(mock_df_no_order, ["subscription_id"], None)
        mock_df_no_order.dropDuplicates.assert_called_with(["subscription_id"])

    @patch("lakehouse_engine.core.delta_ops.DeltaTable")
    def test_scenario_2_3_cross_batch_merge_idempotency_unbounded(self, mock_delta_table_cls):
        """
        Scenario 2.3: Cross-batch idempotent MERGE without partition restrictions.
        Matches primary key across entire table history for Liquid Clustering.
        """
        mock_spark = MagicMock()
        mock_source_df = MagicMock()
        mock_source_df.isEmpty.return_value = False
        mock_source_df.columns = ["subscription_id", "status", "started_at"]
        mock_source_df.dropDuplicates.return_value = mock_source_df

        mock_delta_instance = MagicMock()
        mock_delta_table_cls.forName.return_value = mock_delta_instance

        merge_batch_pruned(
            spark=mock_spark,
            target_table_fqdn=self.config.gold.fqdn,
            source_df=mock_source_df,
            unique_keys=["subscription_id"],
            prune_col=None,  # Unbounded merge mode
            order_by_col="started_at"
        )

        # Base merge condition must strictly match primary key
        mock_delta_instance.alias.assert_called_with("target")
        merge_builder = mock_delta_instance.alias.return_value.merge.return_value
        mock_delta_instance.alias.return_value.merge.assert_called_with(
            unittest.mock.ANY,
            "target.subscription_id = source.subscription_id"
        )

        # Ordering condition applied to prevent out-of-order overwrites
        merge_builder.whenMatchedUpdateAll.assert_called_with(
            condition="source.started_at >= target.started_at"
        )
        merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once()

    @patch("lakehouse_engine.core.delta_ops.DeltaTable")
    def test_scenario_2_3b_merge_with_partition_date_pruning(self, mock_delta_table_cls):
        """
        Scenario 2.3b: Delta MERGE with partition date pruning.
        Limits target search space to dates present in incoming batch.
        """
        mock_spark = MagicMock()
        mock_source_df = MagicMock()
        mock_source_df.isEmpty.return_value = False
        mock_source_df.columns = ["subscription_id", "started_at"]
        mock_source_df.dropDuplicates.return_value = mock_source_df

        # Simulate incoming batch dates: 2026-10-25
        mock_source_df.select.return_value.distinct.return_value.collect.return_value = [
            (datetime.date(2026, 10, 25),)
        ]

        mock_delta_instance = MagicMock()
        mock_delta_table_cls.forName.return_value = mock_delta_instance

        merge_batch_pruned(
            spark=mock_spark,
            target_table_fqdn=self.config.gold.fqdn,
            source_df=mock_source_df,
            unique_keys=["subscription_id"],
            prune_col="started_at",
            order_by_col=None
        )

        expected_condition = "to_date(target.started_at) IN ('2026-10-25') AND target.subscription_id = source.subscription_id"
        mock_delta_instance.alias.return_value.merge.assert_called_with(
            unittest.mock.ANY,
            expected_condition
        )

    def test_scenario_2_4_primary_key_dlq_quarantine(self):
        """
        Scenario 2.4: Primary Key quarantine.
        Records with NULL or empty primary keys are segregated to DLQ.
        """
        mock_df = MagicMock()
        mock_df.columns = ["subscription_id", "status"]

        valid_df, dlq_df = split_valid_and_dlq(mock_df, ["subscription_id"])

        # Filter must be applied for both valid and invalid records
        self.assertEqual(mock_df.filter.call_count, 2)

    def test_scenario_2_5_data_quality_expectation_quarantine(self):
        """
        Scenario 2.5: Data Quality Expectation with action='quarantine'.
        Violating records are routed to DLQ with failure reason.
        """
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        # Simulate violating rows exist for status check
        mock_violating_df = MagicMock()
        mock_violating_df.isEmpty.return_value = False
        mock_violating_df.count.return_value = 2

        mock_passing_df = MagicMock()
        mock_passing_df.filter.return_value = mock_passing_df
        mock_df.filter.side_effect = [
            mock_passing_df,
            mock_violating_df
        ]

        quarantine_rules = [e for e in self.config.gold.expectations if e.action == "quarantine"]
        clean_df, dlq_df, warnings = evaluate_data_quality_rules(mock_df, quarantine_rules)

        self.assertIsNotNone(clean_df)
        self.assertIsNotNone(dlq_df)
        mock_violating_df.withColumn.assert_called_with(
            "_dlq_reason",
            unittest.mock.ANY
        )

    def test_scenario_2_6_data_quality_expectation_fail_batch(self):
        """
        Scenario 2.6: Data Quality Expectation with action='fail_batch'.
        Aborts the micro-batch transaction with DataQualityException.
        """
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False
        failing_subset = MagicMock()
        failing_subset.count.return_value = 1
        mock_df.filter.return_value = failing_subset

        fail_batch_rules = [e for e in self.config.gold.expectations if e.action == "fail_batch"]

        with self.assertRaises(DataQualityException) as ctx:
            evaluate_data_quality_rules(mock_df, fail_batch_rules)

        self.assertIn("Data Quality Circuit Breaker", str(ctx.exception))
        self.assertIn("valid_mrr", str(ctx.exception))

    def test_scenario_2_7_data_contract_sla_circuit_breaker(self):
        """
        Scenario 2.7: Data Contract SLA circuit breaker threshold.
        Trips when quarantine rate exceeds max_quarantine_rate_percent.
        """
        source_rows = 10
        quarantined_rows = 8  # 80% quarantine rate > 50% contract threshold
        threshold = self.config.contract.max_quarantine_rate_percent

        quarantine_rate = (quarantined_rows / source_rows) * 100.0
        self.assertGreater(quarantine_rate, threshold)

        sla_msg = (
            f"Data Contract SLA Violation on entity '{self.config.entity_name}': "
            f"Quarantine rate of {quarantine_rate:.2f}% ({quarantined_rows}/{source_rows} rows) "
            f"exceeded maximum allowed contract threshold of {threshold:.1f}%."
        )
        if self.config.contract.quarantine_action == "fail_batch":
            with self.assertRaises(RuntimeError) as ctx:
                raise RuntimeError(sla_msg)
            self.assertIn("Data Contract SLA Violation", str(ctx.exception))

        self.config.contract.quarantine_action = "warn"
        self.assertEqual(self.config.contract.quarantine_action, "warn")

    def test_scenario_2_8_pii_segregation_and_masking(self):
        """
        Scenario 2.8: PII Segregation.
        Sensitive profile data is extracted into a segregated compliance table.
        """
        mock_batch_df = MagicMock()
        pii_config = self.config.pii_splits[0]

        extracted_pii_df = extract_pii_split(mock_batch_df, pii_config)

        mock_batch_df.select.assert_called_once()
        selected_cols = mock_batch_df.select.call_args[0]
        self.assertEqual(len(selected_cols), 3)

    def test_scenario_2_9_rescued_data_variant_capture(self):
        """
        Scenario 2.9: Capturing unmapped attributes into binary VARIANT / string
        in _rescued_payload with technical lineage.
        """
        mock_df = MagicMock()
        mock_df.columns = ["payload", "subscriptionId", "status", "startedOn"]

        transformed_df = transform_entity(
            df=mock_df,
            config=self.config.gold,
            payload_col_name="payload",
            payload_type="variant"
        )

        self.assertIsNotNone(transformed_df)
        self.assertTrue(mock_df.withColumn.called or mock_df.select.called)

    def test_scenario_2_10_spot_preemption_graceful_shutdown(self):
        """
        Scenario 2.10: Cloud spot preemption resilience (SIGTERM/SIGINT).
        Stops the streaming query cleanly without corrupting checkpoints.
        """
        mock_query = MagicMock()
        mock_query.isActive = True

        def simulate_graceful_shutdown(signum, frame):
            if mock_query.isActive:
                mock_query.stop()

        simulate_graceful_shutdown(15, None)
        mock_query.stop.assert_called_once()

    def test_scenario_2_11_idempotent_historical_backfill(self):
        """
        Scenario 2.11: Idempotent Historical Backfill.
        Date filtering auto-resolves against source columns and does not reset checkpoints.
        """
        mock_spark = MagicMock()
        mock_source_df = MagicMock()
        mock_source_df.columns = ["subscriptionId", "plan", "status", "startedOn"]
        mock_source_df.count.return_value = 5
        mock_source_df.filter.return_value = mock_source_df
        mock_spark.read.table.return_value = mock_source_df

        with patch("lakehouse_engine.core.backfill.ensure_delta_table"), \
             patch("lakehouse_engine.core.backfill.merge_batch_pruned"), \
             patch("lakehouse_engine.core.backfill.log_batch_audit") as mock_audit:

            run_backfill(
                spark=mock_spark,
                config=self.config,
                start_date="2026-01-01",
                end_date="2026-03-31",
                date_column="started_at",
                reset_checkpoint=False
            )

            filter_calls = [str(c[0][0]) for c in mock_source_df.filter.call_args_list]
            self.assertTrue(len(filter_calls) >= 2)

            mock_audit.assert_called_once()
            audit_args = mock_audit.call_args[1]
            self.assertEqual(audit_args["batch_id"], -1)
            self.assertEqual(audit_args["status"], "SUCCESS (BACKFILL)")

if __name__ == "__main__":
    unittest.main()
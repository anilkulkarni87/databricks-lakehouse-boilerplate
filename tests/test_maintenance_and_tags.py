import unittest
import sys
from pathlib import Path
from unittest.mock import MagicMock
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yaml
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.delta_ops import apply_unity_catalog_tags, ensure_delta_table
from lakehouse_engine.core.maintenance import run_table_maintenance

class TestMaintenanceAndTags(unittest.TestCase):
    def setUp(self):
        self.yaml_text = """
version: "1.0"
entity_name: "test_entity"
source:
  catalog: "raw_landing"
  schema: "event_stream"
  table: "test_events"
runtime:
  trigger_type: "AvailableNow"
  checkpoint_location: "dbfs:/checkpoints/test"
  auto_optimize: true
gold:
  catalog: "lake_dw"
  schema: "analytics"
  table: "fct_orders"
  unique_keys: ["order_id"]
  prune_column: "order_date"
  enable_dlq: true
  dlq_table: "lake_dw.analytics.fct_orders_dlq"
  tags:
    data_classification: "internal"
    tier: "mission-critical"
  fields:
    - name: "order_id"
      type: "string"
      expr: "payload.orderId"
pii_splits:
  - name: "customer_pii"
    target_catalog: "lake_dw"
    target_schema: "pii_vault"
    target_table: "dim_customers"
    unique_keys: ["customer_id"]
    foreign_key_expr: "payload.customerId"
    foreign_key_name: "customer_id"
    tags:
      data_classification: "restricted_pii"
    fields:
      - name: "email"
        type: "string"
        expr: "payload.email"
"""
        self.config = EntityPipelineConfig.model_validate(yaml.safe_load(self.yaml_text))

    def test_config_parses_tags_and_auto_optimize(self):
        self.assertTrue(self.config.runtime.auto_optimize)
        self.assertEqual(self.config.gold.tags["tier"], "mission-critical")
        self.assertEqual(self.config.gold.tags["data_classification"], "internal")
        self.assertEqual(self.config.pii_splits[0].tags["data_classification"], "restricted_pii")

    def test_apply_unity_catalog_tags(self):
        mock_spark = MagicMock()
        apply_unity_catalog_tags(
            spark=mock_spark,
            table_fqdn="cat.schema.tbl",
            tags={"tier": "mission-critical", "owner": "platform"}
        )
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        self.assertIn("ALTER TABLE cat.schema.tbl SET TAGS", sql_call)
        self.assertIn("'tier' = 'mission-critical'", sql_call)
        self.assertIn("'owner' = 'platform'", sql_call)

    def test_apply_unity_catalog_tags_empty_noop(self):
        mock_spark = MagicMock()
        apply_unity_catalog_tags(mock_spark, "cat.schema.tbl", {})
        mock_spark.sql.assert_not_called()

    def test_run_table_maintenance_executes_optimize_and_vacuum(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_spark.sql.return_value.isEmpty.return_value = False
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(asDict=lambda: {"numFilesAdded": 2, "numFilesRemoved": 10})
        ]

        report = run_table_maintenance(
            spark=mock_spark,
            config=self.config,
            retention_hours=72
        )

        self.assertEqual(report["entity_name"], "test_entity")
        self.assertEqual(report["tables_maintained"], 3)  # Gold, DLQ, PII split

        # Verify SQL calls included OPTIMIZE and VACUUM
        sql_commands = [c[0][0] for c in mock_spark.sql.call_args_list]
        self.assertTrue(any("OPTIMIZE lake_dw.analytics.fct_orders" in cmd for cmd in sql_commands))
        self.assertTrue(any("VACUUM lake_dw.analytics.fct_orders RETAIN 72 HOURS" in cmd for cmd in sql_commands))
        self.assertTrue(any("OPTIMIZE lake_dw.analytics.fct_orders_dlq" in cmd for cmd in sql_commands))
        self.assertTrue(any("OPTIMIZE lake_dw.pii_vault.dim_customers" in cmd for cmd in sql_commands))

if __name__ == "__main__":
    unittest.main()

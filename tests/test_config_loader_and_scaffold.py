import os
import sys
import unittest
from pathlib import Path

src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from lakehouse_engine.core.config_loader import substitute_env_vars, load_pipeline_config
from lakehouse_engine.core.scaffold import infer_sql_type, is_pii_field, generate_entity_scaffold_yaml
from lakehouse_engine.models.config import EntityPipelineConfig

class TestConfigLoaderAndScaffold(unittest.TestCase):

    def test_substitute_env_vars_with_defaults(self):
        template = "catalog: ${MY_CATALOG:-dev_catalog}, path: ${MY_PATH:-/default/path}"
        result = substitute_env_vars(template, {})
        self.assertEqual(result, "catalog: dev_catalog, path: /default/path")

    def test_substitute_env_vars_with_custom_dict(self):
        template = "catalog: ${MY_CATALOG:-dev_catalog}, path: ${MY_PATH}"
        result = substitute_env_vars(template, {"MY_CATALOG": "prod_catalog", "MY_PATH": "/prod/path"})
        self.assertEqual(result, "catalog: prod_catalog, path: /prod/path")

    def test_substitute_env_vars_missing_raises_error(self):
        template = "catalog: ${UNSET_VARIABLE}"
        with self.assertRaises(ValueError) as ctx:
            substitute_env_vars(template, {})
        self.assertIn("UNSET_VARIABLE", str(ctx.exception))

    def test_load_pipeline_config_with_interpolation(self):
        yaml_content = """
version: "1.0"
entity_name: "test_entity"
source:
  catalog: "${TEST_CAT:-test_catalog}"
  schema: "raw"
  table: "events"
runtime:
  checkpoint_location: "/Volumes/${TEST_CAT:-test_catalog}/default/checkpoints/test"
gold:
  catalog: "${TEST_CAT:-test_catalog}"
  schema: "default"
  table: "dim_test"
  unique_keys: ["id"]
  fields:
    - name: "id"
      type: "string"
      expr: "id"
"""
        config = load_pipeline_config(yaml_content, {"TEST_CAT": "analytics_prod"})
        self.assertEqual(config.source.catalog, "analytics_prod")
        self.assertEqual(config.gold.catalog, "analytics_prod")
        self.assertEqual(config.runtime.checkpoint_location, "/Volumes/analytics_prod/default/checkpoints/test")

    def test_scaffold_inference_and_pii_detection(self):
        self.assertEqual(infer_sql_type(123), "int")
        self.assertEqual(infer_sql_type(99999999999), "bigint")
        self.assertEqual(infer_sql_type(45.67), "double")
        self.assertEqual(infer_sql_type(True), "boolean")
        self.assertEqual(infer_sql_type("2026-10-23T12:00:00"), "timestamp")
        self.assertEqual(infer_sql_type({"k": "v"}), "variant")

        self.assertTrue(is_pii_field("subscriber_email"))
        self.assertTrue(is_pii_field("contact_phone"))
        self.assertTrue(is_pii_field("tax_identifier"))
        self.assertFalse(is_pii_field("order_amount"))

    def test_scaffold_yaml_generation_validates(self):
        sample = {
            "orderId": "ord_1001",
            "orderTotal": 99.50,
            "isGift": False,
            "customerEmail": "customer@example.com",
            "metadata": {"source": "mobile_app"}
        }
        yaml_str = generate_entity_scaffold_yaml(
            entity_name="orders",
            source_table="raw_landing.orders_events",
            sample_record=sample
        )
        # Check that generated YAML loads into EntityPipelineConfig cleanly
        config = load_pipeline_config(yaml_str, {"CATALOG": "main", "CHECKPOINT_ROOT": "/Volumes/main/default"})
        self.assertEqual(config.entity_name, "orders")
        self.assertEqual(len(config.pii_splits), 1)
        self.assertEqual(config.pii_splits[0].fields[0].name, "customerEmail")
        self.assertIn("orderId", config.gold.unique_keys)

if __name__ == "__main__":
    unittest.main()

import unittest
import json
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.mcp.server import (
    handle_validate,
    handle_list_entities,
    handle_generate_scaffold,
    TOOLS_METADATA
)

class TestMCPAndVariant(unittest.TestCase):
    def test_variant_config(self):
        config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        with open(config_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        self.assertEqual(config.source.payload_type, "variant")
        self.assertTrue(config.gold.store_rescued_as_variant)

    def test_mcp_tools_metadata(self):
        tool_names = [t["name"] for t in TOOLS_METADATA]
        self.assertIn("validate_pipeline_config", tool_names)
        self.assertIn("list_entities", tool_names)
        self.assertIn("export_genie_instructions", tool_names)
        self.assertIn("generate_entity_scaffold", tool_names)

    def test_mcp_validate_pipeline_config(self):
        config_path = str(Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml")
        res = handle_validate({"config_path": config_path})
        self.assertFalse(res.get("isError", False))
        payload = json.loads(res["content"][0]["text"])
        self.assertEqual(payload["status"], "VALID")
        self.assertEqual(payload["entity_name"], "subscriptions")
        self.assertEqual(payload["expectations_count"], 3)

    def test_mcp_list_entities(self):
        configs_dir = str(Path(__file__).parents[1] / "configs" / "examples")
        res = handle_list_entities({"configs_dir": configs_dir})
        self.assertFalse(res.get("isError", False))
        entities = json.loads(res["content"][0]["text"])
        self.assertIsInstance(entities, list)
        self.assertGreaterEqual(len(entities), 1)
        entity_names = [e["entity_name"] for e in entities]
        self.assertIn("subscriptions", entity_names)

    def test_mcp_generate_scaffold_and_validate(self):
        sample_payload = {
            "invoice_id": "inv_101",
            "amount_cents": 5000,
            "is_paid": True,
            "customer_email": "billing@corp.com"
        }
        res = handle_generate_scaffold({
            "entity_name": "invoices",
            "source_table": "raw_billing.silver_invoices",
            "target_table": "finance_dw.billing.fact_invoices",
            "sample_json": json.dumps(sample_payload)
        })
        self.assertFalse(res.get("isError", False))
        scaffold_yaml = res["content"][0]["text"]
        
        # Verify generated scaffold validates against EntityPipelineConfig schema
        parsed = yaml.safe_load(scaffold_yaml)
        config = EntityPipelineConfig.model_validate(parsed)
        self.assertEqual(config.entity_name, "invoices")
        self.assertTrue(config.gold.enable_dlq)
        self.assertEqual(len(config.pii_splits), 1)
        self.assertEqual(config.pii_splits[0].fields[0].name, "customer_email")

if __name__ == "__main__":
    unittest.main()

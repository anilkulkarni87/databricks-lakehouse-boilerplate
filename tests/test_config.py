import unittest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

class TestConfig(unittest.TestCase):
    def test_subscriptions_config_validates(self):
        config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        with open(config_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        self.assertEqual(config.entity_name, "subscriptions")
        self.assertEqual(config.gold.fqdn, "saas_dw.subscription_analytics.dim_subscriptions")
        self.assertIsNotNone(config.gold.table_comment)
        self.assertIn("Curated SaaS subscription dimension", config.gold.table_comment)
        self.assertEqual(len(config.gold.fields), 12)

        # Verify column comments
        mrr_field = next(f for f in config.gold.fields if f.name == "mrr_amount")
        self.assertIsNotNone(mrr_field.description)
        self.assertIn("Monthly Recurring Revenue", mrr_field.description)

        # Verify Kimball & Genie Extensions
        self.assertEqual(len(config.gold.foreign_keys), 1)
        self.assertEqual(config.gold.foreign_keys[0].column, "subscriber_account_id")
        self.assertEqual(len(config.gold.metrics), 3)
        self.assertEqual(config.gold.metrics[0].name, "Total MRR")
        self.assertIn("status = 'ACTIVE'", config.gold.metrics[0].sql_formula)

        self.assertEqual(len(config.pii_splits), 1)
        self.assertEqual(config.pii_splits[0].fqdn, "saas_dw.subscription_analytics_pii.dim_subscriber_profiles")
        self.assertIsNotNone(config.pii_splits[0].table_comment)

    def test_template_config_validates(self):
        template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
        with open(template_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        self.assertEqual(config.entity_name, "example_entity")
        self.assertEqual(config.runtime.trigger_type, "AvailableNow")
        self.assertIsNotNone(config.gold.table_comment)

if __name__ == "__main__":
    unittest.main()

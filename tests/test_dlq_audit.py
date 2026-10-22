import unittest
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

class TestDLQAudit(unittest.TestCase):
    def test_dlq_and_audit_config_features(self):
        config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        with open(config_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        
        # Audit table assertions
        self.assertEqual(config.runtime.audit_table, "saas_dw.operational_telemetry.batch_audit_log")
        
        # DLQ assertions
        self.assertTrue(config.gold.enable_dlq)
        self.assertEqual(config.gold.dlq_fqdn, "saas_dw.subscription_analytics.dim_subscriptions_dlq")
        self.assertTrue(config.gold.track_rescued_data)

    def test_template_dlq_and_audit_features(self):
        template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
        with open(template_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        self.assertIsNotNone(config.runtime.audit_table)
        self.assertTrue(config.gold.enable_dlq)
        self.assertTrue(config.gold.track_rescued_data)

if __name__ == "__main__":
    unittest.main()

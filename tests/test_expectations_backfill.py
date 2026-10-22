import unittest
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

class TestExpectationsBackfill(unittest.TestCase):
    def test_expectations_config(self):
        config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        with open(config_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        
        # Assert expectations parsing
        self.assertEqual(len(config.gold.expectations), 3)
        
        # Assert fail_batch expectation
        fail_exp = next(e for e in config.gold.expectations if e.action == "fail_batch")
        self.assertEqual(fail_exp.name, "valid_mrr_non_negative")
        self.assertIn("mrr_amount >= 0.0", fail_exp.expr)
        
        # Assert quarantine expectation
        quarantine_exp = next(e for e in config.gold.expectations if e.action == "quarantine")
        self.assertEqual(quarantine_exp.name, "valid_subscription_status")
        self.assertIn("status IN", quarantine_exp.expr)

        # Assert warn expectation
        warn_exp = next(e for e in config.gold.expectations if e.action == "warn")
        self.assertEqual(warn_exp.name, "non_future_started_timestamp")

    def test_template_expectations(self):
        template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
        with open(template_path) as f:
            data = yaml.safe_load(f)

        config = EntityPipelineConfig.model_validate(data)
        self.assertEqual(len(config.gold.expectations), 2)
        self.assertEqual(config.gold.expectations[0].action, "fail_batch")
        self.assertEqual(config.gold.expectations[1].action, "quarantine")

if __name__ == "__main__":
    unittest.main()

import unittest
import json
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.triage import (
    analyze_quarantined_records,
    apply_triage_patches_to_yaml
)

class TestDLQTriage(unittest.TestCase):
    def setUp(self):
        config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        with open(config_path) as f:
            data = yaml.safe_load(f)
        self.config = EntityPipelineConfig.model_validate(data)

    def test_zero_record_triage(self):
        report = analyze_quarantined_records(self.config, [])
        self.assertEqual(report.total_quarantined_records, 0)
        self.assertEqual(len(report.findings), 0)
        self.assertIn("100% clean", report.format_console_report())

    def test_schema_drift_primary_key_candidate(self):
        sample_dlq = [
            {
                "_dlq_reason": "MISSING_OR_EMPTY_KEY:subscription_id",
                "_dlq_timestamp": "2026-10-22T14:30:00Z",
                "_rescued_payload": {
                    "sub_id": "sub_drift_101",
                    "plan": {"tier": "Pro"},
                    "subscriberId": "cust_999"
                }
            }
        ]
        report = analyze_quarantined_records(self.config, sample_dlq)
        self.assertEqual(report.total_quarantined_records, 1)
        self.assertEqual(len(report.findings), 1)
        
        finding = report.findings[0]
        self.assertIn("sub_id", finding.root_cause)
        self.assertIsNotNone(finding.suggested_yaml_patch)
        self.assertEqual(finding.suggested_yaml_patch["field"], "subscription_id")
        self.assertIn("coalesce", finding.suggested_yaml_patch["new_expr"])
        self.assertIn("payload.sub_id", finding.suggested_yaml_patch["new_expr"])

    def test_categorical_expectation_violation(self):
        sample_dlq = [
            {
                "_dlq_reason": "FAILED_EXPECTATION:valid_subscription_status (status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED'))",
                "_dlq_timestamp": "2026-10-22T14:32:00Z",
                "_rescued_payload": {
                    "subscriptionId": "sub_555",
                    "status": "PENDING_APPROVAL"
                }
            }
        ]
        report = analyze_quarantined_records(self.config, sample_dlq)
        self.assertEqual(report.total_quarantined_records, 1)
        self.assertEqual(len(report.findings), 1)

        finding = report.findings[0]
        self.assertIn("PENDING_APPROVAL", finding.root_cause)
        self.assertIsNotNone(finding.suggested_yaml_patch)
        self.assertEqual(finding.suggested_yaml_patch["expectation"], "valid_subscription_status")
        self.assertIn("PENDING_APPROVAL", finding.suggested_yaml_patch["new_expr"])

    def test_apply_triage_patches_to_yaml(self):
        sample_dlq = [
            {
                "_dlq_reason": "FAILED_EXPECTATION:valid_subscription_status (status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED'))",
                "_dlq_timestamp": "2026-10-22T14:32:00Z",
                "_rescued_payload": {
                    "subscriptionId": "sub_555",
                    "status": "TRIALING"
                }
            }
        ]
        report = analyze_quarantined_records(self.config, sample_dlq)
        
        # Test applying to temporary copy
        test_file = Path(__file__).parents[1] / "configs" / "examples" / "temp_triage_test.yml"
        src_file = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
        test_file.write_text(src_file.read_text())
        
        try:
            success, diff = apply_triage_patches_to_yaml(str(test_file), report)
            self.assertTrue(success)
            self.assertIn("TRIALING", diff)
            
            # Verify patched config remains valid against Pydantic schema
            patched_data = yaml.safe_load(test_file.read_text())
            patched_config = EntityPipelineConfig.model_validate(patched_data)
            exp = next(e for e in patched_config.gold.expectations if e.name == "valid_subscription_status")
            self.assertIn("TRIALING", exp.expr)
        finally:
            if test_file.exists():
                test_file.unlink()

if __name__ == "__main__":
    unittest.main()

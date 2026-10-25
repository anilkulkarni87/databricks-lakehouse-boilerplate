import unittest
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yaml
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.guardrails import lint_pipeline_config
from lakehouse_engine.core.config_loader import load_pipeline_config, resolve_secret_value, substitute_secrets
from lakehouse_engine.core.triage import classify_schema_drift
from lakehouse_engine.core.alerts import build_contract_breach_slack_payload, build_volume_anomaly_slack_payload

class TestGuardrailsAndSecrets(unittest.TestCase):
    def setUp(self):
        self.base_yaml = """
version: "1.0"
entity_name: "test_entity"
source:
  catalog: "raw_landing"
  schema: "event_stream"
  table: "test_events"
  payload_column: "payload"
  payload_type: "variant"
runtime:
  trigger_type: "AvailableNow"
  checkpoint_location: "dbfs:/checkpoints/test"
gold:
  catalog: "lake_dw"
  schema: "analytics"
  table: "fct_orders"
  write_mode: "merge"
  deduplication_mode: "in_batch"
  order_by_col: "order_date"
  unique_keys: ["order_id"]
  prune_column: "order_date"
  fields:
    - name: "order_id"
      type: "string"
      expr: "payload.orderId"
    - name: "order_date"
      type: "date"
      expr: "cast(payload.orderDate as date)"
"""

    def test_clean_config_passes_guardrails(self):
        data = yaml.safe_load(self.base_yaml)
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertTrue(report.is_valid, report.format_console_report())
        self.assertEqual(report.error_count, 0)

    def test_unbounded_merge_triggers_error(self):
        data = yaml.safe_load(self.base_yaml)
        data["gold"]["prune_column"] = None
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertFalse(report.is_valid)
        self.assertTrue(any(f.rule == "UNBOUNDED_MERGE_PROHIBITED" for f in report.findings))

    def test_unbounded_merge_with_override_flag_passes_with_warning(self):
        data = yaml.safe_load(self.base_yaml)
        data["gold"]["prune_column"] = None
        data["guardrails"] = {"allow_unbounded_merge": True}
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertTrue(report.is_valid, report.format_console_report())
        self.assertTrue(any(f.rule == "UNBOUNDED_MERGE_OVERRIDE" and f.level == "WARNING" for f in report.findings))

    def test_non_deterministic_dedup_caught(self):
        data = yaml.safe_load(self.base_yaml)
        data["gold"]["fields"] = [
            {"name": "order_id", "type": "string", "expr": "payload.orderId"},
            {"name": "amount", "type": "double", "expr": "payload.amount"}
        ]
        data["gold"]["order_by_col"] = None
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertFalse(report.is_valid)
        self.assertTrue(any(f.rule == "NON_DETERMINISTIC_DEDUPLICATION" for f in report.findings))

    def test_pii_leakage_strict_mode_triggers_error(self):
        data = yaml.safe_load(self.base_yaml)
        data["gold"]["fields"].append({
            "name": "customer_email",
            "type": "string",
            "expr": "payload.customer.email"
        })
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertFalse(report.is_valid)
        self.assertTrue(any(f.rule == "PII_LEAKAGE_DETECTED" and f.level == "ERROR" for f in report.findings))

    def test_pii_masked_or_hashed_passes(self):
        data = yaml.safe_load(self.base_yaml)
        data["gold"]["fields"].append({
            "name": "customer_email",
            "type": "string",
            "expr": "payload.customer.email",
            "mask_policy": "mask_email"
        })
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertTrue(report.is_valid, report.format_console_report())

    def test_invalid_secret_reference_caught_by_guardrails(self):
        data = yaml.safe_load(self.base_yaml)
        data["secrets"] = {"invalid_secret": "plain_text_password_123"}
        config = EntityPipelineConfig.model_validate(data)
        report = lint_pipeline_config(config)
        self.assertFalse(report.is_valid)
        self.assertTrue(any(f.rule == "INVALID_SECRET_REFERENCE" for f in report.findings))

    def test_secret_resolution_via_mock_store(self):
        raw = "webhook: {{secrets/app_scope/slack_token}}"
        resolved = substitute_secrets(raw, secrets_store={"app_scope/slack_token": "xoxb-mock-token-99"})
        self.assertEqual(resolved, "webhook: xoxb-mock-token-99")

    def test_secret_resolution_via_env_fallback(self):
        os.environ["SECRET_ALERT_SCOPE_PAGERDUTY_KEY"] = "pd-key-xyz"
        try:
            val = resolve_secret_value("alert_scope", "pagerduty_key")
            self.assertEqual(val, "pd-key-xyz")
        finally:
            del os.environ["SECRET_ALERT_SCOPE_PAGERDUTY_KEY"]

    def test_secret_resolution_missing_raises_error(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_secret_value("unknown_scope", "nonexistent_key")
        self.assertIn("Secret scope reference", str(ctx.exception))

    def test_internal_secret_expansion_in_config_loader(self):
        yaml_content = """
version: "1.0"
entity_name: "test_secrets"
secrets:
  slack_token: "mock-slack-token"
source:
  catalog: "raw"
  schema: "stream"
  table: "events"
runtime:
  trigger_type: "AvailableNow"
  checkpoint_location: "dbfs:/chk"
  alert_webhook:
    enabled: true
    url: "https://hooks.slack.com/services/${secrets.slack_token}"
gold:
  catalog: "lake"
  schema: "dw"
  table: "test"
  unique_keys: ["id"]
  prune_column: "date"
  fields:
    - name: "id"
      type: "string"
      expr: "payload.id"
    - name: "date"
      type: "date"
      expr: "current_date()"
"""
        config = load_pipeline_config(yaml_content)
        self.assertEqual(config.runtime.alert_webhook.url, "https://hooks.slack.com/services/mock-slack-token")

    def test_drift_classification_compatible_additive(self):
        target = {"id": "string", "amount": "double"}
        incoming = {"id": "string", "amount": "double", "currency": "string"}
        res = classify_schema_drift(target, incoming, primary_keys=["id"])
        self.assertEqual(res.classification, "COMPATIBLE_ADDITIVE")
        self.assertFalse(res.is_breaking)
        self.assertIn("currency", res.added_columns)

    def test_drift_classification_breaking_type_mismatch(self):
        target = {"id": "string", "amount": "double"}
        incoming = {"id": "string", "amount": "string"}
        res = classify_schema_drift(target, incoming, primary_keys=["id"])
        self.assertEqual(res.classification, "BREAKING_TYPE_MISMATCH")
        self.assertTrue(res.is_breaking)
        self.assertIn("amount", res.type_mismatches)

    def test_drift_classification_breaking_missing_primary_key(self):
        target = {"id": "string", "amount": "double"}
        incoming = {"order_id": "string", "amount": "double"}
        res = classify_schema_drift(target, incoming, primary_keys=["id"])
        self.assertEqual(res.classification, "BREAKING_KEY_CHANGE")
        self.assertTrue(res.is_breaking)

    def test_contract_breach_slack_payload(self):
        payload = build_contract_breach_slack_payload(
            entity_name="subscriptions",
            batch_id=42,
            quarantine_rate=12.5,
            threshold_percent=5.0,
            quarantined_count=125,
            source_count=1000,
            channel="#alerts-critical"
        )
        self.assertIn("DATA CONTRACT SLA BREACH", payload["blocks"][0]["text"]["text"])
        self.assertEqual(payload["channel"], "#alerts-critical")
        self.assertIn("12.50%", payload["blocks"][1]["fields"][0]["text"])

    def test_volume_anomaly_slack_payload(self):
        payload = build_volume_anomaly_slack_payload(
            entity_name="orders",
            batch_id=101,
            current_count=100,
            baseline_count=1000.0,
            anomaly_type="DROP",
            percentage_change=-90.0
        )
        self.assertIn("Volume Anomaly (DROP)", payload["blocks"][0]["text"]["text"])
        self.assertIn("-90.0%", payload["blocks"][1]["fields"][2]["text"])

if __name__ == "__main__":
    unittest.main()

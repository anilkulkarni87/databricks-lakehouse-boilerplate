import sys
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from lakehouse_engine.models.config import AlertWebhookConfig, EntityPipelineConfig
from lakehouse_engine.core.alerts import build_slack_alert_payload, send_dlq_webhook_alert
from lakehouse_engine.core.config_loader import load_pipeline_config

class TestAlerts(unittest.TestCase):

    def test_build_slack_alert_payload(self):
        payload = build_slack_alert_payload(
            entity_name="subscriptions",
            batch_id=42,
            quarantined_count=5,
            dlq_table="main.default.dim_subscriptions_dlq",
            channel="#data-alerts"
        )
        self.assertIn("subscriptions", payload["text"])
        self.assertEqual(payload["channel"], "#data-alerts")
        self.assertEqual(len(payload["blocks"]), 3)
        self.assertIn("dim_subscriptions_dlq", str(payload["blocks"]))

    def test_alert_disabled_returns_false_no_network(self):
        cfg = AlertWebhookConfig(enabled=False, url="https://hooks.slack.com/services/fake")
        result = send_dlq_webhook_alert(cfg, "subscriptions", 1, 10, "dlq_tbl")
        self.assertFalse(result)

    def test_alert_under_threshold_returns_false(self):
        cfg = AlertWebhookConfig(enabled=True, url="https://hooks.slack.com/services/fake", min_quarantined_threshold=5)
        result = send_dlq_webhook_alert(cfg, "subscriptions", 1, 3, "dlq_tbl")
        self.assertFalse(result)

    @patch("urllib.request.urlopen")
    def test_alert_success(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        cfg = AlertWebhookConfig(enabled=True, url="https://hooks.slack.com/services/fake", min_quarantined_threshold=1)
        result = send_dlq_webhook_alert(cfg, "subscriptions", 1, 5, "dlq_tbl")
        self.assertTrue(result)
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_alert_network_failure_does_not_raise(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Network unreachable")

        cfg = AlertWebhookConfig(enabled=True, url="https://hooks.slack.com/services/fake")
        result = send_dlq_webhook_alert(cfg, "subscriptions", 1, 5, "dlq_tbl")
        self.assertFalse(result)

    def test_config_loader_with_alert_webhook(self):
        yaml_content = """
version: "1.0"
entity_name: "subscriptions"
source:
  catalog: "main"
  schema: "raw"
  table: "events"
runtime:
  checkpoint_location: "/Volumes/main/default/checkpoints/sub"
  alert_webhook:
    enabled: true
    url: "${SLACK_URL:-https://hooks.slack.com/services/default}"
    channel: "#lakehouse-alerts"
    min_quarantined_threshold: 2
gold:
  catalog: "main"
  schema: "default"
  table: "dim_sub"
  unique_keys: ["id"]
  fields:
    - name: "id"
      type: "string"
      expr: "id"
"""
        config = load_pipeline_config(yaml_content, {"SLACK_URL": "https://hooks.slack.com/services/custom"})
        self.assertIsNotNone(config.runtime.alert_webhook)
        self.assertTrue(config.runtime.alert_webhook.enabled)
        self.assertEqual(config.runtime.alert_webhook.url, "https://hooks.slack.com/services/custom")
        self.assertEqual(config.runtime.alert_webhook.channel, "#lakehouse-alerts")
        self.assertEqual(config.runtime.alert_webhook.min_quarantined_threshold, 2)

if __name__ == "__main__":
    unittest.main()

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from lakehouse_engine.models.config import AlertWebhookConfig

logger = logging.getLogger(__name__)

def build_slack_alert_payload(
    entity_name: str,
    batch_id: int,
    quarantined_count: int,
    dlq_table: str,
    channel: Optional[str] = None
) -> Dict[str, Any]:
    """
    Constructs a rich Slack Block Kit / incoming webhook JSON payload for DLQ alerts.
    """
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    headline = f":warning: *Lakehouse DLQ Alert: `{entity_name}` (Batch #{batch_id})*"
    
    payload = {
        "text": f"Lakehouse Alert: {quarantined_count} records quarantined for entity '{entity_name}'",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": headline
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Quarantined Rows:*\n`{quarantined_count}`"},
                    {"type": "mrkdwn", "text": f"*DLQ Table:*\n`{dlq_table}`"},
                    {"type": "mrkdwn", "text": f"*Entity:*\n`{entity_name}`"},
                    {"type": "mrkdwn", "text": f"*Timestamp:*\n{utc_now}"}
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Run triage command: `lakehouse-engine triage-dlq -c configs/entities/{entity_name}.yaml`"
                    }
                ]
            }
        ]
    }

    if channel:
        payload["channel"] = channel

    return payload

def build_contract_breach_slack_payload(
    entity_name: str,
    batch_id: int,
    quarantine_rate: float,
    threshold_percent: float,
    quarantined_count: int,
    source_count: int,
    channel: Optional[str] = None
) -> Dict[str, Any]:
    """
    Constructs a high-urgency Slack Block Kit alert for Data Contract SLA breaches.
    """
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    headline = f":rotating_light: *DATA CONTRACT SLA BREACH: `{entity_name}` (Batch #{batch_id})*"
    
    payload = {
        "text": f"SLA Breach: Entity '{entity_name}' quarantined {quarantine_rate:.1f}% of records (threshold: {threshold_percent:.1f}%)",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": headline
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Quarantine Rate:*\n`{quarantine_rate:.2f}%` (Threshold: {threshold_percent:.1f}%)"},
                    {"type": "mrkdwn", "text": f"*Quarantined / Total:*\n`{quarantined_count:,} / {source_count:,} rows`"},
                    {"type": "mrkdwn", "text": f"*Entity:*\n`{entity_name}`"},
                    {"type": "mrkdwn", "text": f"*Timestamp:*\n{utc_now}"}
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": ":fire: Circuit breaker tripped to prevent corrupt data propagation. Inspect DLQ immediately."
                    }
                ]
            }
        ]
    }
    if channel:
        payload["channel"] = channel
    return payload

def build_volume_anomaly_slack_payload(
    entity_name: str,
    batch_id: int,
    current_count: int,
    baseline_count: float,
    anomaly_type: str,  # "DROP" or "SPIKE"
    percentage_change: float,
    channel: Optional[str] = None
) -> Dict[str, Any]:
    """
    Constructs an operational Slack Block Kit alert for batch volume anomalies.
    """
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    icon = ":chart_with_downwards_trend:" if anomaly_type == "DROP" else ":chart_with_upwards_trend:"
    headline = f"{icon} *Operational Volume Anomaly ({anomaly_type}): `{entity_name}` (Batch #{batch_id})*"
    
    payload = {
        "text": f"Volume Anomaly: {entity_name} recorded {anomaly_type} of {abs(percentage_change):.1f}% vs baseline",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": headline
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Current Batch Volume:*\n`{current_count:,} records`"},
                    {"type": "mrkdwn", "text": f"*Rolling Baseline:*\n`{int(baseline_count):,} records`"},
                    {"type": "mrkdwn", "text": f"*Deviation:*\n`{percentage_change:+.1f}%`"},
                    {"type": "mrkdwn", "text": f"*Timestamp:*\n{utc_now}"}
                ]
            }
        ]
    }
    if channel:
        payload["channel"] = channel
    return payload

def _post_webhook(url: str, payload: Dict[str, Any], timeout_seconds: int = 10) -> bool:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            status_code = response.getcode()
            return 200 <= status_code < 300
    except urllib.error.URLError as e:
        logger.warning(f"Failed to deliver webhook alert to {url}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error while sending webhook: {e}")
        return False

def send_dlq_webhook_alert(
    alert_config: AlertWebhookConfig,
    entity_name: str,
    batch_id: int,
    quarantined_count: int,
    dlq_table: str,
    timeout_seconds: int = 10
) -> bool:
    if not alert_config.enabled or not alert_config.url:
        return False

    if quarantined_count < alert_config.min_quarantined_threshold:
        return False

    payload = build_slack_alert_payload(
        entity_name=entity_name,
        batch_id=batch_id,
        quarantined_count=quarantined_count,
        dlq_table=dlq_table,
        channel=alert_config.channel
    )
    return _post_webhook(alert_config.url, payload, timeout_seconds)

def send_contract_breach_webhook_alert(
    alert_config: AlertWebhookConfig,
    entity_name: str,
    batch_id: int,
    quarantine_rate: float,
    threshold_percent: float,
    quarantined_count: int,
    source_count: int,
    timeout_seconds: int = 10
) -> bool:
    if not alert_config.enabled or not alert_config.url:
        return False

    payload = build_contract_breach_slack_payload(
        entity_name=entity_name,
        batch_id=batch_id,
        quarantine_rate=quarantine_rate,
        threshold_percent=threshold_percent,
        quarantined_count=quarantined_count,
        source_count=source_count,
        channel=alert_config.channel
    )
    return _post_webhook(alert_config.url, payload, timeout_seconds)

def send_volume_anomaly_webhook_alert(
    alert_config: AlertWebhookConfig,
    entity_name: str,
    batch_id: int,
    current_count: int,
    baseline_count: float,
    anomaly_type: str,
    percentage_change: float,
    timeout_seconds: int = 10
) -> bool:
    if not alert_config.enabled or not alert_config.url:
        return False

    payload = build_volume_anomaly_slack_payload(
        entity_name=entity_name,
        batch_id=batch_id,
        current_count=current_count,
        baseline_count=baseline_count,
        anomaly_type=anomaly_type,
        percentage_change=percentage_change,
        channel=alert_config.channel
    )
    return _post_webhook(alert_config.url, payload, timeout_seconds)

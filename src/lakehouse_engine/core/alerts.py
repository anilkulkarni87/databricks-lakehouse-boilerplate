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
    Constructs a rich Slack Block Kit / incoming webhook JSON payload.
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

def send_dlq_webhook_alert(
    alert_config: AlertWebhookConfig,
    entity_name: str,
    batch_id: int,
    quarantined_count: int,
    dlq_table: str,
    timeout_seconds: int = 10
) -> bool:
    """
    Dispatches a real-time HTTP POST webhook notification when records are quarantined.
    Returns True if sent successfully, False otherwise. Never raises an exception to caller.
    """
    if not alert_config.enabled or not alert_config.url:
        return False

    if quarantined_count < alert_config.min_quarantined_threshold:
        logger.debug(
            f"Quarantined count {quarantined_count} below threshold {alert_config.min_quarantined_threshold}. Alert skipped."
        )
        return False

    payload = build_slack_alert_payload(
        entity_name=entity_name,
        batch_id=batch_id,
        quarantined_count=quarantined_count,
        dlq_table=dlq_table,
        channel=alert_config.channel
    )

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        alert_config.url,
        data=data,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            status_code = response.getcode()
            if 200 <= status_code < 300:
                logger.info(f"Successfully dispatched DLQ alert webhook to {alert_config.url} (status: {status_code})")
                return True
            else:
                logger.warning(f"DLQ alert webhook returned non-2xx status: {status_code}")
                return False
    except urllib.error.URLError as e:
        logger.warning(f"Failed to deliver DLQ alert webhook to {alert_config.url}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error while sending DLQ alert webhook: {e}")
        return False

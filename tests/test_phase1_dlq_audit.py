import pytest
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

def test_phase1_config_features():
    config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
    with open(config_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    
    # Audit table assertions
    assert config.runtime.audit_table == "saas_dw.operational_telemetry.batch_audit_log"
    
    # DLQ assertions
    assert config.gold.enable_dlq is True
    assert config.gold.dlq_fqdn == "saas_dw.subscription_analytics.dim_subscriptions_dlq"
    assert config.gold.track_rescued_data is True

def test_template_phase1_features():
    template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
    with open(template_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert config.runtime.audit_table is not None
    assert config.gold.enable_dlq is True
    assert config.gold.track_rescued_data is True

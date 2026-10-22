import pytest
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

def test_subscriptions_config_validates():
    config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
    with open(config_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert config.entity_name == "subscriptions"
    assert config.gold.fqdn == "saas_dw.subscription_analytics.dim_subscriptions"
    assert len(config.gold.fields) == 12
    assert len(config.pii_splits) == 1
    assert config.pii_splits[0].fqdn == "saas_dw.subscription_analytics_pii.dim_subscriber_profiles"

def test_template_config_validates():
    template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
    with open(template_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert config.entity_name == "example_entity"
    assert config.runtime.trigger_type == "AvailableNow"

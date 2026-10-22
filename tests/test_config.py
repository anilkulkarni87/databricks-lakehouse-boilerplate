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
    assert config.gold.table_comment is not None
    assert "Curated SaaS subscription dimension" in config.gold.table_comment
    assert len(config.gold.fields) == 12

    # Verify column comments
    mrr_field = next(f for f in config.gold.fields if f.name == "mrr_amount")
    assert mrr_field.description is not None
    assert "Monthly Recurring Revenue" in mrr_field.description

    # Verify Kimball & Genie Extensions
    assert len(config.gold.foreign_keys) == 1
    assert config.gold.foreign_keys[0].column == "subscriber_account_id"
    assert len(config.gold.metrics) == 3
    assert config.gold.metrics[0].name == "Total MRR"
    assert "status = 'ACTIVE'" in config.gold.metrics[0].sql_formula

    assert len(config.pii_splits) == 1
    assert config.pii_splits[0].fqdn == "saas_dw.subscription_analytics_pii.dim_subscriber_profiles"
    assert config.pii_splits[0].table_comment is not None

def test_template_config_validates():
    template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
    with open(template_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert config.entity_name == "example_entity"
    assert config.runtime.trigger_type == "AvailableNow"
    assert config.gold.table_comment is not None

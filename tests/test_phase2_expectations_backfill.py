import pytest
import yaml
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig

def test_phase2_expectations_config():
    config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
    with open(config_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    
    # Assert expectations parsing
    assert len(config.gold.expectations) == 3
    
    # Assert fail_batch expectation
    fail_exp = next(e for e in config.gold.expectations if e.action == "fail_batch")
    assert fail_exp.name == "valid_mrr_non_negative"
    assert "mrr_amount >= 0.0" in fail_exp.expr
    
    # Assert quarantine expectation
    quarantine_exp = next(e for e in config.gold.expectations if e.action == "quarantine")
    assert quarantine_exp.name == "valid_subscription_status"
    assert "status IN" in quarantine_exp.expr

    # Assert warn expectation
    warn_exp = next(e for e in config.gold.expectations if e.action == "warn")
    assert warn_exp.name == "non_future_started_timestamp"

def test_template_phase2_expectations():
    template_path = Path(__file__).parents[1] / "configs" / "templates" / "entity_template.yml"
    with open(template_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert len(config.gold.expectations) == 2
    assert config.gold.expectations[0].action == "fail_batch"
    assert config.gold.expectations[1].action == "quarantine"

def test_variant_config():
    config_path = Path(__file__).parents[1] / "configs" / "examples" / "subscriptions.yml"
    with open(config_path) as f:
        data = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(data)
    assert config.source.payload_type == "variant"
    assert config.gold.store_rescued_as_variant is True

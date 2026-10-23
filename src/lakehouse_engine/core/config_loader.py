import os
import re
import yaml
from pathlib import Path
from typing import Dict, Optional, Union, Any
from lakehouse_engine.models.config import EntityPipelineConfig

# Matches ${VAR_NAME} or ${VAR_NAME:-default_value}
ENV_VAR_PATTERN = re.compile(r"\$\{\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*:-\s*([^}]*))?\s*\}")

def substitute_env_vars(raw_text: str, env_vars: Optional[Dict[str, str]] = None) -> str:
    """
    Substitutes environment variables in a configuration string.
    Supports syntax:
      - ${VAR_NAME}
      - ${VAR_NAME:-default_value}
    """
    effective_env = dict(os.environ)
    if env_vars:
        effective_env.update(env_vars)

    def _replace_match(match: re.Match) -> str:
        var_name = match.group(1)
        default_val = match.group(2)
        
        if var_name in effective_env and effective_env[var_name] is not None:
            return effective_env[var_name]
        if default_val is not None:
            return default_val
        raise ValueError(
            f"Environment variable '{var_name}' is required in configuration but is not set, and no default value was provided."
        )

    return ENV_VAR_PATTERN.sub(_replace_match, raw_text)

def load_pipeline_config(
    config_source: Union[str, Path],
    env_vars: Optional[Dict[str, str]] = None
) -> EntityPipelineConfig:
    """
    Loads and validates an EntityPipelineConfig from a YAML file path or raw YAML string,
    performing environment variable interpolation first.
    """
    if isinstance(config_source, Path):
        raw_text = config_source.read_text(encoding="utf-8")
    elif isinstance(config_source, str) and "\n" not in config_source:
        path = Path(config_source)
        if not path.is_file():
            raise FileNotFoundError(f"Configuration file not found: {path.resolve()}")
        raw_text = path.read_text(encoding="utf-8")
    else:
        raw_text = str(config_source)

    interpolated_yaml = substitute_env_vars(raw_text, env_vars)
    raw_dict = yaml.safe_load(interpolated_yaml)

    if not isinstance(raw_dict, dict):
        raise ValueError(f"Invalid YAML configuration; expected mapping, got {type(raw_dict).__name__}")

    return EntityPipelineConfig.model_validate(raw_dict)

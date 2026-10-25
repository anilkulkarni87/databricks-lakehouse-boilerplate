import os
import re
import yaml
from pathlib import Path
from typing import Dict, Optional, Union, Any
from lakehouse_engine.models.config import EntityPipelineConfig

# Matches ${VAR_NAME} or ${VAR_NAME:-default_value}
ENV_VAR_PATTERN = re.compile(r"\$\{\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*:-\s*([^}]*))?\s*\}")

# Matches {{secrets/scope/key}} or {{ secrets/scope/key }}
SECRET_PATTERN = re.compile(r"\{\{\s*secrets\/([a-zA-Z0-9_\-]+)\/([a-zA-Z0-9_\-]+)\s*\}\}")

# Matches ${secrets.key}
INTERNAL_SECRET_REF_PATTERN = re.compile(r"\$\{\s*secrets\.([a-zA-Z0-9_]+)\s*\}")

def _clean_env_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).upper()

def resolve_secret_value(
    scope: str,
    key: str,
    secrets_store: Optional[Dict[str, str]] = None
) -> str:
    """
    Resolves a secret value following an enterprise priority hierarchy:
      1. Explicit secrets_store dictionary (mock / injection for CI & local tests)
      2. Databricks dbutils.secrets.get(scope, key) when running inside Databricks Runtime
      3. Environment variable fallback: SECRET_<SCOPE>_<KEY> or <KEY>
    """
    lookup_keys = [
        f"{scope}/{key}",
        f"{scope}.{key}",
        f"{scope}:{key}"
    ]
    if secrets_store:
        for lk in lookup_keys:
            if lk in secrets_store and secrets_store[lk] is not None:
                return secrets_store[lk]

    # Attempt Databricks native dbutils resolution
    try:
        import IPython
        ipython = IPython.get_ipython()
        if ipython and "dbutils" in ipython.user_ns:
            dbutils = ipython.user_ns["dbutils"]
            return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass

    try:
        import __main__
        if hasattr(__main__, "dbutils"):
            return __main__.dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass

    # Local / CI Environment Variable Fallback
    env_candidate_1 = f"SECRET_{_clean_env_name(scope)}_{_clean_env_name(key)}"
    env_candidate_2 = f"{_clean_env_name(scope)}_{_clean_env_name(key)}"
    env_candidate_3 = _clean_env_name(key)

    for env_cand in [env_candidate_1, env_candidate_2, env_candidate_3]:
        if env_cand in os.environ and os.environ[env_cand] is not None:
            return os.environ[env_cand]

    raise ValueError(
        f"Secret scope reference '{{{{secrets/{scope}/{key}}}}}' could not be resolved.\n"
        f"  - In Databricks: verify scope '{scope}' and key '{key}' exist in Databricks Secret Manager.\n"
        f"  - In Local/CI: set environment variable '{env_candidate_1}' or pass mock secrets_store."
    )

def substitute_secrets(
    raw_text: str,
    secrets_store: Optional[Dict[str, str]] = None
) -> str:
    def _replace_secret_match(match: re.Match) -> str:
        scope = match.group(1)
        key = match.group(2)
        return resolve_secret_value(scope, key, secrets_store)

    return SECRET_PATTERN.sub(_replace_secret_match, raw_text)

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
    env_vars: Optional[Dict[str, str]] = None,
    secrets_store: Optional[Dict[str, str]] = None,
    allow_unresolved_secrets: bool = False
) -> EntityPipelineConfig:
    """
    Loads and validates an EntityPipelineConfig from a YAML file path or raw YAML string,
    performing environment variable interpolation and secret scope resolution.
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

    # 1. Substitute environment variables
    interpolated_yaml = substitute_env_vars(raw_text, env_vars)

    # 2. Substitute secret scopes
    if allow_unresolved_secrets:
        try:
            interpolated_yaml = substitute_secrets(interpolated_yaml, secrets_store)
        except ValueError:
            # Leave placeholders intact for offline linting
            pass
    else:
        interpolated_yaml = substitute_secrets(interpolated_yaml, secrets_store)

    raw_dict = yaml.safe_load(interpolated_yaml)

    if not isinstance(raw_dict, dict):
        raise ValueError(f"Invalid YAML configuration; expected mapping, got {type(raw_dict).__name__}")

    # 3. Resolve internal ${secrets.<key>} references cleanly in-memory
    if "secrets" in raw_dict and isinstance(raw_dict["secrets"], dict):
        top_secrets = raw_dict["secrets"]
        def _resolve_in_memory(obj: Any) -> Any:
            if isinstance(obj, str):
                return INTERNAL_SECRET_REF_PATTERN.sub(
                    lambda m: str(top_secrets.get(m.group(1), m.group(0))), obj
                )
            elif isinstance(obj, dict):
                return {k: _resolve_in_memory(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_resolve_in_memory(elem) for elem in obj]
            return obj

        raw_dict = _resolve_in_memory(raw_dict)

    return EntityPipelineConfig.model_validate(raw_dict)

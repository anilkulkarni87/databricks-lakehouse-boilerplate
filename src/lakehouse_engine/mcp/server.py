import sys
import json
import yaml
from pathlib import Path
from typing import Dict, Any, List
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.genie import generate_genie_space_documentation

TOOLS_METADATA = [
    {
        "name": "validate_pipeline_config",
        "description": "Validates a Lakehouse Engine YAML configuration against the Pydantic schema.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "yaml_content": {
                    "type": "string",
                    "description": "Raw YAML string of the pipeline configuration"
                },
                "config_path": {
                    "type": "string",
                    "description": "Optional file path to YAML configuration"
                }
            }
        }
    },
    {
        "name": "list_entities",
        "description": "Scans and lists all configured entities, tables, and PII splits in the repository.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "configs_dir": {
                    "type": "string",
                    "description": "Directory to search for YAML configurations (defaults to 'configs/examples')",
                    "default": "configs/examples"
                }
            }
        }
    },
    {
        "name": "export_genie_instructions",
        "description": "Compiles all repository entity configs into Databricks Genie Space instructions and Unity Catalog constraint DDLs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "configs_dir": {
                    "type": "string",
                    "description": "Directory containing entity YAML configs",
                    "default": "configs/examples"
                }
            }
        }
    },
    {
        "name": "generate_entity_scaffold",
        "description": "Analyzes a sample JSON payload and generates a starter YAML configuration with inferred Spark SQL types and candidate PII detection.",
        "inputSchema": {
            "type": "object",
            "required": ["entity_name", "source_table", "target_table", "sample_json"],
            "properties": {
                "entity_name": {"type": "string", "description": "Name of the entity (e.g. orders, subscriptions)"},
                "source_table": {"type": "string", "description": "Source Silver table FQDN (e.g. raw.landing.events)"},
                "target_table": {"type": "string", "description": "Target Gold table FQDN (e.g. enterprise.dw.dim_orders)"},
                "sample_json": {"type": "string", "description": "Sample JSON string payload"}
            }
        }
    }
]

def handle_validate(args: Dict[str, Any]) -> Dict[str, Any]:
    content = args.get("yaml_content")
    if not content and args.get("config_path"):
        content = Path(args["config_path"]).read_text()

    if not content:
        return {"isError": True, "content": [{"type": "text", "text": "Must provide either yaml_content or config_path."}]}

    try:
        data = yaml.safe_load(content)
        config = EntityPipelineConfig.model_validate(data)
        summary = {
            "status": "VALID",
            "entity_name": config.entity_name,
            "gold_table": config.gold.fqdn,
            "fields_count": len(config.gold.fields),
            "pii_splits": [p.fqdn for p in config.pii_splits],
            "expectations_count": len(config.gold.expectations),
            "metrics_count": len(config.gold.metrics),
            "foreign_keys_count": len(config.gold.foreign_keys)
        }
        return {"content": [{"type": "text", "text": json.dumps(summary, indent=2)}]}
    except Exception as e:
        return {"isError": True, "content": [{"type": "text", "text": f"Configuration Validation Error:\n{str(e)}"}]}

def handle_list_entities(args: Dict[str, Any]) -> Dict[str, Any]:
    configs_dir = Path(args.get("configs_dir", "configs/examples"))
    if not configs_dir.is_absolute():
        repo_root = Path(__file__).parents[3]
        configs_dir = repo_root / configs_dir

    yaml_files = list(configs_dir.glob("*.yml")) + list(configs_dir.glob("*.yaml"))
    entities = []

    for yf in yaml_files:
        try:
            data = yaml.safe_load(yf.read_text())
            if data and "entity_name" in data:
                cfg = EntityPipelineConfig.model_validate(data)
                entities.append({
                    "entity_name": cfg.entity_name,
                    "file_path": str(yf.name),
                    "domain": cfg.business_domain,
                    "source_table": cfg.source.fqdn,
                    "gold_table": cfg.gold.fqdn,
                    "table_type": cfg.gold.table_type,
                    "pii_tables": [p.fqdn for p in cfg.pii_splits],
                    "expectations": [e.name for e in cfg.gold.expectations],
                    "metrics": [m.name for m in cfg.gold.metrics]
                })
        except Exception as e:
            entities.append({"file": str(yf.name), "error": str(e)})

    return {"content": [{"type": "text", "text": json.dumps(entities, indent=2)}]}

def handle_export_genie(args: Dict[str, Any]) -> Dict[str, Any]:
    configs_dir = Path(args.get("configs_dir", "configs/examples"))
    if not configs_dir.is_absolute():
        repo_root = Path(__file__).parents[3]
        configs_dir = repo_root / configs_dir

    yaml_files = list(configs_dir.glob("*.yml")) + list(configs_dir.glob("*.yaml"))
    configs = []
    for yf in yaml_files:
        data = yaml.safe_load(yf.read_text())
        if data and "entity_name" in data:
            configs.append(EntityPipelineConfig.model_validate(data))

    doc = generate_genie_space_documentation(configs)
    return {"content": [{"type": "text", "text": doc}]}

def handle_generate_scaffold(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sample = json.loads(args["sample_json"])
    except Exception as e:
        return {"isError": True, "content": [{"type": "text", "text": f"Invalid JSON string: {e}"}]}

    entity_name = args["entity_name"]
    source_table = args["source_table"]
    target_table = args["target_table"]

    # Parse target catalog/schema/table
    parts = target_table.split(".")
    catalog = parts[0] if len(parts) == 3 else "spark_catalog"
    schema = parts[1] if len(parts) == 3 else (parts[0] if len(parts) == 2 else "default")
    table = parts[-1]

    src_parts = source_table.split(".")
    src_cat = src_parts[0] if len(src_parts) == 3 else "spark_catalog"
    src_sch = src_parts[1] if len(src_parts) == 3 else (src_parts[0] if len(src_parts) == 2 else "default")
    src_tbl = src_parts[-1]

    fields = []
    pii_fields = []

    def infer_type(val):
        if isinstance(val, bool): return "boolean"
        if isinstance(val, int): return "bigint"
        if isinstance(val, float): return "double"
        if isinstance(val, (dict, list)): return "variant"
        return "string"

    for k, v in sample.items():
        spark_type = infer_type(v)
        is_pii = any(term in k.lower() for term in ["email", "phone", "name", "address", "ssn", "tax", "secret"])

        field_spec = {
            "name": k,
            "type": spark_type,
            "expr": f"payload.{k}",
            "description": f"Field extracted from {k}"
        }

        if is_pii:
            pii_fields.append(field_spec)
        else:
            fields.append(field_spec)

    scaffold = {
        "version": "1.0",
        "entity_name": entity_name,
        "source": {
            "catalog": src_cat,
            "schema": src_sch,
            "table": src_tbl,
            "payload_column": "payload",
            "payload_type": "variant"
        },
        "runtime": {
            "trigger_type": "AvailableNow",
            "max_mbytes_per_trigger": 2000,
            "checkpoint_location": f"dbfs:/checkpoints/lakehouse_engine/{entity_name}",
            "audit_table": f"{catalog}.operational_telemetry.batch_audit_log"
        },
        "gold": {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "table_comment": f"Curated dimensional entity for {entity_name}",
            "table_type": "dimension",
            "unique_keys": [fields[0]["name"] if fields else "id"],
            "cluster_by": ["created_at"],
            "enable_dlq": True,
            "track_rescued_data": True,
            "fields": fields
        }
    }

    if pii_fields:
        scaffold["pii_splits"] = [{
            "name": f"{entity_name}_pii",
            "target_catalog": catalog,
            "target_schema": f"{schema}_pii",
            "target_table": f"{table}_pii",
            "table_comment": f"Restricted PII attributes for {entity_name}",
            "unique_keys": ["entity_hash_id"],
            "foreign_key_expr": f"xxhash64(cast(payload.{fields[0]['name'] if fields else 'id'} as string))",
            "foreign_key_name": "entity_hash_id",
            "fields": pii_fields
        }]

    yaml_str = yaml.dump(scaffold, sort_keys=False)
    return {"content": [{"type": "text", "text": yaml_str}]}

def run_mcp_server():
    """
    Standard Model Context Protocol (MCP) server listening on stdin / stdout via JSON-RPC 2.0.
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            req = json.loads(line)
        except Exception as e:
            continue

        msg_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            res = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "lakehouse-engine-mcp",
                        "version": "0.1.0"
                    }
                }
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()

        elif method == "notifications/initialized":
            pass

        elif method == "ping":
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}) + "\n")
            sys.stdout.flush()

        elif method == "tools/list":
            res = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"tools": TOOLS_METADATA}
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()

        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            if tool_name == "validate_pipeline_config":
                out = handle_validate(tool_args)
            elif tool_name == "list_entities":
                out = handle_list_entities(tool_args)
            elif tool_name == "export_genie_instructions":
                out = handle_export_genie(tool_args)
            elif tool_name == "generate_entity_scaffold":
                out = handle_generate_scaffold(tool_args)
            else:
                out = {"isError": True, "content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}]}

            res = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": out
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()
        else:
            res = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32601, "message": f"Method {method} not found"}
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    run_mcp_server()

# Model Context Protocol (MCP) Server for Lakehouse Engine

This guide details the native, lightweight **Model Context Protocol (MCP)** server built directly into `lakehouse-engine`. It allows AI agents (in Claude Desktop, Cursor, Antigravity IDE, or custom autonomous agents) to interact with your Lakehouse metadata, validate configurations, enforce Kimball dimensional standards, and generate Databricks Genie assets.

---

## 1. Why a Native MCP Server?

Rather than requiring human data engineers to manually copy-paste schema YAMLs, debug validation errors, or transcribe column comments into Genie prompts, the MCP server provides a standardized JSON-RPC 2.0 interface over `stdio`.

- **Zero Heavy Dependencies**: Runs without requiring PySpark or Java installed locally.
- **Instant CI/Agent Integration**: Plug-and-play with any standard MCP client using `.mcp.json`.
- **Self-Governing Agentic Loop**: AI agents can scaffold a new entity YAML, validate it against Pydantic rules, and export Genie documentation in a single autonomous turn.

---

## 2. Available MCP Tools

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| `validate_pipeline_config` | `config_path` *(string)* or `yaml_content` *(string)* | Validates a Lakehouse Engine YAML config against the Pydantic schema. Returns validation status, field count, PII splits, expectations, and metrics. |
| `list_entities` | `configs_dir` *(string, default: `configs/examples`)* | Scans the repository and returns an inventory of all configured entities, gold tables, and PII splits. |
| `export_genie_instructions` | `configs_dir` *(string)*, `output_file` *(optional)* | Compiles all entity configurations into a Databricks Genie Space system prompt and constraint DDLs. |
| `generate_entity_scaffold` | `entity_name` *(string)*, `source_table` *(string)*, `gold_table` *(string)*, `primary_key` *(string)* | Generates a complete, compliant YAML configuration template with DLQ, expectations, and PII split placeholders. |

---

## 3. Configuring Your AI Client

A pre-configured `.mcp.json` file is included at the root of the repository:

```json
{
  "mcpServers": {
    "lakehouse-engine": {
      "command": "python3",
      "args": ["-m", "lakehouse_engine.cli", "mcp-serve"],
      "env": {
        "PYTHONPATH": "src"
      }
    }
  }
}
```

### Running Manually via CLI or Task
```bash
# Via Taskfile
task mcp:serve

# Via CLI directly
PYTHONPATH=src python3 -m lakehouse_engine.cli mcp-serve
```

---

## 4. Example AI Interaction Flow

1. **User asks AI**: *"Create a new entity config for invoice events and ensure it follows our lakehouse standards."*
2. **AI calls `generate_entity_scaffold`**:
   - Generates initial YAML with DLQ, audit, and expectation templates.
3. **AI customizes fields & calls `validate_pipeline_config`**:
   - Pydantic schema validation runs instantly.
   - If a rule is violated (e.g., missing `action` on expectation), AI sees the error and self-corrects.
4. **AI calls `export_genie_instructions`**:
   - Refreshes the repository's Genie instructions markdown artifact.

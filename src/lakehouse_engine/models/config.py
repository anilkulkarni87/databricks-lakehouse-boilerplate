from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, model_validator

class FieldDefinition(BaseModel):
    name: str = Field(..., description="Target column name")
    type: str = Field(..., description="Spark SQL data type")
    expr: str = Field(..., description="SQL transformation expression")
    description: Optional[str] = Field(None, description="Semantic description for Unity Catalog / Genie Text-to-SQL")
    comment: Optional[str] = Field(None, description="Alternative alias for column description")
    is_primary_key: bool = Field(False, description="Whether this column is part of the primary key")
    mask_policy: Optional[str] = Field(None, description="Unity Catalog Column Mask UDF name")

    @model_validator(mode="after")
    def sync_comment_and_description(self):
        if not self.description and self.comment:
            self.description = self.comment
        elif not self.comment and self.description:
            self.comment = self.description
        return self

class ForeignKeyReference(BaseModel):
    column: str = Field(..., description="Foreign key column name in this table")
    referenced_table: str = Field(..., description="Target dimension table (catalog.schema.table)")
    referenced_column: str = Field(..., description="Target primary key column in dimension table")
    relationship_description: Optional[str] = Field(None, description="Explains relationship for Genie text-to-SQL")

class MetricDefinition(BaseModel):
    name: str = Field(..., description="Business metric name (e.g., Total MRR)")
    sql_formula: str = Field(..., description="SQL aggregation expression (e.g., SUM(mrr_amount))")
    description: str = Field(..., description="Business definition and guidance for Genie queries")
    synonyms: List[str] = Field(default_factory=list, description="Common alternative names used in natural language")

class ExpectationConfig(BaseModel):
    name: str = Field(..., description="Data quality check name")
    expr: str = Field(..., description="Boolean SQL validation expression that must evaluate to TRUE")
    action: str = Field("quarantine", description="Action on failure: 'quarantine' (route to DLQ), 'warn' (log), 'fail_batch' (halt)")
    description: Optional[str] = Field(None, description="Explanation of why this rule exists")

class SourceConfig(BaseModel):
    catalog: str
    schema_name: str = Field(..., alias="schema")
    table: str
    stream_filter_field: Optional[str] = None
    stream_filter_value: Optional[str] = None
    payload_column: str = Field("payload", description="Name of the semi-structured payload column in source")
    payload_type: str = Field("variant", description="Encoding: 'variant' (Databricks native binary VARIANT), 'json_string', or 'struct'")

    @property
    def fqdn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}" if self.catalog else f"{self.schema_name}.{self.table}"

class AlertWebhookConfig(BaseModel):
    enabled: bool = Field(False, description="Enable real-time webhook alerting on DLQ quarantine or batch failures")
    url: Optional[str] = Field(None, description="Webhook URL (Slack, MS Teams, PagerDuty, or custom endpoint). Supports ")
    channel: Optional[str] = Field(None, description="Optional Slack / Teams channel name")
    min_quarantined_threshold: int = Field(1, description="Minimum number of quarantined rows in a batch before triggering an alert")
    include_summary: bool = Field(True, description="Include root-cause breakdown in the alert message")
    channel_type: str = Field("slack", description="Alert channel type: 'slack' (Block Kit) or 'generic' (JSON)")
    volume_anomaly_alerts: bool = Field(False, description="Enable automated volume drop/spike anomaly detection")
    volume_drop_threshold_percent: float = Field(75.0, description="Alert if micro-batch drops more than X% below baseline volume")
    volume_spike_threshold_percent: float = Field(300.0, description="Alert if micro-batch surges more than X% above baseline volume")

class RuntimeConfig(BaseModel):
    trigger_type: str = "AvailableNow"
    max_mbytes_per_trigger: int = 2000
    checkpoint_location: str
    auto_optimize: bool = Field(True, description="Enable automatic Delta write optimization and auto-compaction")
    audit_table: Optional[str] = Field(None, description="Centralized operational telemetry audit table FQDN")
    alert_webhook: Optional[AlertWebhookConfig] = Field(default_factory=AlertWebhookConfig, description="Optional real-time alert webhook configuration")

class GuardrailsConfig(BaseModel):
    enforce_primary_key_order_by: bool = Field(True, description="Requires an ordering column for in-batch deduplication")
    require_prune_column_on_merge: bool = Field(True, description="Requires partition/date pruning column on Delta merge")
    pii_leakage_detection: str = Field("strict", description="PII column check: 'strict' (fails on unsegregated PII), 'warn', or 'ignore'")
    allow_unbounded_merge: bool = Field(False, description="Explicit override flag allowing unpruned full-table merge for small tables")
    enforce_expectations_action: bool = Field(True, description="Requires all data quality rules to have explicit actions")

class DataContractConfig(BaseModel):
    schema_drift_policy: str = Field("fail_on_incompatible", description="Drift policy: 'fail_on_incompatible', 'evolve', or 'quarantine_row'")
    max_quarantine_rate_percent: float = Field(100.0, description="Maximum allowable DLQ quarantine percentage per batch before tripping circuit breaker")
    quarantine_action: str = Field("fail_batch", description="Action on SLA breach: 'fail_batch' or 'warn'")
    enforce_not_null_pk: bool = Field(True, description="Strict contract enforcement that primary keys must not be null")

class TargetTableConfig(BaseModel):
    catalog: str
    schema_name: str = Field(..., alias="schema")
    table: str
    table_comment: Optional[str] = Field(None, description="Semantic description of the table for catalog & Genie")
    comment: Optional[str] = Field(None, description="Alternative alias for table_comment")
    table_type: str = Field("dimension", description="'fact' or 'dimension' for Kimball modeling")
    unique_keys: List[str]
    cluster_by: List[str] = []
    prune_column: Optional[str] = None
    schema_evolution: str = "mergeSchema"
    write_mode: str = Field("merge", description="Target write strategy: 'merge' (upsert) or 'append'")
    deduplication_mode: str = Field("in_batch", description="In-batch deduplication: 'in_batch' or 'none'")
    order_by_col: Optional[str] = Field(None, description="Column used to pick winner in in-batch deduplication")
    lookback_days: Optional[int] = Field(None, description="Optional lookback window in days for partition pruning")
    fields: List[FieldDefinition]
    foreign_keys: List[ForeignKeyReference] = []
    metrics: List[MetricDefinition] = []
    tags: Dict[str, str] = Field(default_factory=dict, description="Unity Catalog governance and compliance tags")

    # Phase 1: DLQ & Rescued Data
    enable_dlq: bool = Field(True, description="Enable automated Dead-Letter Queue quarantine for malformed records")
    dlq_table: Optional[str] = Field(None, description="Custom DLQ table FQDN. Defaults to <target_table>_dlq")
    track_rescued_data: bool = Field(True, description="Preserves raw unmapped payload in _rescued_payload")
    store_rescued_as_variant: bool = Field(True, description="Stores _rescued_payload as Databricks native VARIANT")

    # Phase 2: Data Quality Expectations
    expectations: List[ExpectationConfig] = Field(default_factory=list, description="Declarative data quality rules")

    @property
    def fqdn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}" if self.catalog else f"{self.schema_name}.{self.table}"

    @property
    def dlq_fqdn(self) -> str:
        return self.dlq_table or f"{self.fqdn}_dlq"

class PiiSplitConfig(BaseModel):
    name: str
    target_catalog: str
    target_schema: str
    target_table: str
    table_comment: Optional[str] = Field(None, description="Semantic description of the PII table for catalog & compliance")
    unique_keys: List[str]
    foreign_key_expr: str
    foreign_key_name: str
    cluster_by: List[str] = []
    tags: Dict[str, str] = Field(default_factory=dict, description="Unity Catalog governance and compliance tags")
    fields: List[FieldDefinition]

    @property
    def fqdn(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}" if self.target_catalog else f"{self.target_schema}.{self.target_table}"

class EntityPipelineConfig(BaseModel):
    version: str = "1.0"
    entity_name: str
    description: Optional[str] = None
    business_domain: Optional[str] = Field(None, description="Domain classification (e.g. Finance, Subscriptions, Sales)")
    secrets: Dict[str, str] = Field(default_factory=dict, description="Secret scope references (e.g. {{secrets/scope/key}})")
    guardrails: GuardrailsConfig = Field(default_factory=GuardrailsConfig, description="Fail-fast static config safety guardrails")
    contract: Optional[DataContractConfig] = Field(None, description="Data contract SLA and schema drift policy")
    source: SourceConfig
    runtime: RuntimeConfig
    gold: TargetTableConfig
    pii_splits: List[PiiSplitConfig] = []

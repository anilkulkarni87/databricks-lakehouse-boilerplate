from typing import List, Optional
from pydantic import BaseModel, Field

class FieldDefinition(BaseModel):
    name: str = Field(..., description="Target column name")
    type: str = Field(..., description="Spark SQL data type")
    expr: str = Field(..., description="SQL transformation expression")
    mask_policy: Optional[str] = Field(None, description="Unity Catalog Column Mask UDF name")

class SourceConfig(BaseModel):
    catalog: str
    schema_name: str = Field(..., alias="schema")
    table: str
    stream_filter_field: Optional[str] = None
    stream_filter_value: Optional[str] = None

    @property
    def fqdn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"

class RuntimeConfig(BaseModel):
    trigger_type: str = "AvailableNow"
    max_mbytes_per_trigger: int = 2000
    checkpoint_location: str

class TargetTableConfig(BaseModel):
    catalog: str
    schema_name: str = Field(..., alias="schema")
    table: str
    unique_keys: List[str]
    cluster_by: List[str] = []
    prune_column: Optional[str] = None
    schema_evolution: str = "mergeSchema"
    fields: List[FieldDefinition]

    @property
    def fqdn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"

class PiiSplitConfig(BaseModel):
    name: str
    target_catalog: str
    target_schema: str
    target_table: str
    unique_keys: List[str]
    foreign_key_expr: str
    foreign_key_name: str
    cluster_by: List[str] = []
    fields: List[FieldDefinition]

    @property
    def fqdn(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

class EntityPipelineConfig(BaseModel):
    version: str = "1.0"
    entity_name: str
    description: Optional[str] = None
    source: SourceConfig
    runtime: RuntimeConfig
    gold: TargetTableConfig
    pii_splits: List[PiiSplitConfig] = []

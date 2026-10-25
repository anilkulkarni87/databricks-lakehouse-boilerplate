import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
import yaml

from lakehouse_engine.models.config import EntityPipelineConfig

PII_COLUMN_REGEX = re.compile(
    r"(?i)^(.*_)?(email|ssn|phone|tax_id|tax_identifier|credit_card|card_number|social_security|passport|national_id|date_of_birth|dob)(_.*)?$"
)
HASH_EXPR_REGEX = re.compile(r"(?i)(xxhash|sha2|sha1|md5|mask_)")
SECRET_PATTERN_REGEX = re.compile(r"^\{\{\s*secrets\/([a-zA-Z0-9_\-]+)\/([a-zA-Z0-9_\-]+)\s*\}\}$")

@dataclass
class LintFinding:
    level: str  # "ERROR" or "WARNING"
    rule: str
    message: str
    suggestion: str
    target: Optional[str] = None

@dataclass
class LintReport:
    entity_name: str
    file_path: Optional[str]
    findings: List[LintFinding] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(f.level == "ERROR" for f in self.findings)

    @property
    def error_count(self) -> int:
        return sum(1 for f in self.findings if f.level == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.level == "WARNING")

    def format_console_report(self) -> str:
        status_symbol = "✅ PASS" if self.is_valid else "❌ FAIL"
        lines = [
            "=" * 80,
            f" 🛡️ LAKEHOUSE ENGINE CONFIG GUARDRAILS: {self.entity_name} [{status_symbol}]",
            "=" * 80,
            f" File:     {self.file_path or 'Dynamic Configuration'}",
            f" Errors:   {self.error_count}   |   Warnings: {self.warning_count}",
            "-" * 80
        ]

        if not self.findings:
            lines.append("✨ All static safety and architectural guardrails passed perfectly!")
        else:
            for idx, finding in enumerate(self.findings, 1):
                icon = "❌ [ERROR]" if finding.level == "ERROR" else "⚠️  [WARN] "
                lines.append(f"{icon} {finding.rule}: {finding.message}")
                if finding.target:
                    lines.append(f"    Target:     {finding.target}")
                lines.append(f"    Remedy:     {finding.suggestion}")
                lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)

def lint_pipeline_config(config: EntityPipelineConfig, file_path: Optional[str] = None) -> LintReport:
    report = LintReport(entity_name=config.entity_name, file_path=file_path)

    # 1. Delta Merge & Prune Column Guardrail
    if config.gold.write_mode.lower() == "merge":
        if not config.gold.unique_keys:
            report.findings.append(LintFinding(
                level="ERROR",
                rule="MISSING_UNIQUE_KEYS",
                message=f"Delta write_mode 'merge' on table '{config.gold.fqdn}' requires at least one key in unique_keys.",
                suggestion="Specify one or more natural or primary business keys in 'gold.unique_keys'.",
                target=config.gold.fqdn
            ))

        if not config.gold.prune_column:
            if not config.guardrails.allow_unbounded_merge:
                report.findings.append(LintFinding(
                    level="ERROR",
                    rule="UNBOUNDED_MERGE_PROHIBITED",
                    message=(
                        f"Missing 'prune_column' on gold table '{config.gold.fqdn}'. "
                        "In production, unpruned Delta merges scan the entire table history on every micro-batch."
                    ),
                    suggestion=(
                        "Set 'gold.prune_column' to a date/timestamp field (e.g. 'event_date' or 'started_at'). "
                        "If this table is a tiny lookup (<10k rows), explicitly override via 'guardrails.allow_unbounded_merge: true'."
                    ),
                    target=config.gold.fqdn
                ))
            else:
                report.findings.append(LintFinding(
                    level="WARNING",
                    rule="UNBOUNDED_MERGE_OVERRIDE",
                    message=f"Table '{config.gold.fqdn}' is configured for unbounded merge via explicit override flag.",
                    suggestion="Monitor table size over time to ensure merge latency does not degrade.",
                    target=config.gold.fqdn
                ))

    # 2. In-Batch Deduplication Determinism Guardrail
    if config.gold.deduplication_mode.lower() == "in_batch" and config.gold.unique_keys:
        has_order_col = bool(config.gold.order_by_col)
        standard_timestamp_cols = {"updated_at", "event_timestamp", "timestamp", "created_at", "started_at"}
        present_field_names = {f.name.lower() for f in config.gold.fields}
        has_implicit_order = bool(standard_timestamp_cols.intersection(present_field_names))

        if not has_order_col and not has_implicit_order:
            if config.guardrails.enforce_primary_key_order_by:
                report.findings.append(LintFinding(
                    level="ERROR",
                    rule="NON_DETERMINISTIC_DEDUPLICATION",
                    message=(
                        f"In-batch deduplication on unique_keys {config.gold.unique_keys} without an ordering column "
                        "causes Spark to drop duplicate records non-deterministically."
                    ),
                    suggestion=(
                        "Define 'gold.order_by_col' (e.g. 'event_timestamp') or add a standard 'updated_at' "
                        "timestamp column so the latest event is reliably retained."
                    ),
                    target=config.gold.fqdn
                ))
            else:
                report.findings.append(LintFinding(
                    level="WARNING",
                    rule="NON_DETERMINISTIC_DEDUPLICATION_WARNING",
                    message="In-batch deduplication lacks an explicit ordering column.",
                    suggestion="Define 'gold.order_by_col' to ensure deterministic latest-wins semantics.",
                    target=config.gold.fqdn
                ))

    # 3. PII Leakage Detection Guardrail
    pii_detection_mode = config.guardrails.pii_leakage_detection.lower()
    if pii_detection_mode != "ignore":
        for field_def in config.gold.fields:
            if PII_COLUMN_REGEX.match(field_def.name):
                is_masked = bool(field_def.mask_policy)
                is_hashed = bool(HASH_EXPR_REGEX.search(field_def.expr))

                if not is_masked and not is_hashed:
                    level = "ERROR" if pii_detection_mode == "strict" else "WARNING"
                    report.findings.append(LintFinding(
                        level=level,
                        rule="PII_LEAKAGE_DETECTED",
                        message=(
                            f"Column '{field_def.name}' in Gold table '{config.gold.fqdn}' matches PII pattern "
                            "but is stored raw without a Unity Catalog mask_policy or cryptographic hash expression."
                        ),
                        suggestion=(
                            f"Move '{field_def.name}' to 'pii_splits' in a restricted schema, apply a Unity Catalog "
                            f"'mask_policy: mask_email', or hash via 'xxhash64(expr)'. (Override with guardrails.pii_leakage_detection: warn|ignore)."
                        ),
                        target=f"{config.gold.fqdn}.{field_def.name}"
                    ))

    # 4. Secret Scope Reference Syntax Guardrail
    for secret_name, secret_val in config.secrets.items():
        if not SECRET_PATTERN_REGEX.match(secret_val):
            report.findings.append(LintFinding(
                level="ERROR",
                rule="INVALID_SECRET_REFERENCE",
                message=(
                    f"Secret '{secret_name}' has invalid reference syntax: '{secret_val}'. "
                    "Must match Databricks secret scope syntax: '{{secrets/<scope>/<key>}}'."
                ),
                suggestion="Update value to follow '{{secrets/my_scope/my_key}}' format.",
                target=f"secrets.{secret_name}"
            ))

    # 5. Expectation Sanity Guardrail
    for exp in config.gold.expectations:
        if not exp.name or not exp.name.strip():
            report.findings.append(LintFinding(
                level="ERROR",
                rule="INVALID_EXPECTATION_NAME",
                message="Expectation rule defined with empty or whitespace-only name.",
                suggestion="Provide a descriptive name such as 'valid_mrr_non_negative'.",
                target="gold.expectations"
            ))
        if not exp.expr or not exp.expr.strip():
            report.findings.append(LintFinding(
                level="ERROR",
                rule="EMPTY_EXPECTATION_EXPR",
                message=f"Expectation '{exp.name}' has an empty SQL validation expression.",
                suggestion="Provide a valid Boolean SQL condition (e.g. 'amount >= 0.0').",
                target=f"gold.expectations.{exp.name}"
            ))
        if exp.action not in ("quarantine", "fail_batch", "warn"):
            report.findings.append(LintFinding(
                level="ERROR",
                rule="INVALID_EXPECTATION_ACTION",
                message=f"Expectation '{exp.name}' has invalid action '{exp.action}'.",
                suggestion="Set action to one of: 'quarantine', 'fail_batch', or 'warn'.",
                target=f"gold.expectations.{exp.name}"
            ))

    # 6. Data Contract Sanity Guardrail
    if config.contract:
        if config.contract.max_quarantine_rate_percent < 0.0 or config.contract.max_quarantine_rate_percent > 100.0:
            report.findings.append(LintFinding(
                level="ERROR",
                rule="INVALID_CONTRACT_QUARANTINE_RATE",
                message=(
                    f"Contract max_quarantine_rate_percent ({config.contract.max_quarantine_rate_percent}) "
                    "must be between 0.0 and 100.0 percent."
                ),
                suggestion="Set max_quarantine_rate_percent to a realistic SLA threshold (e.g. 5.0).",
                target="contract.max_quarantine_rate_percent"
            ))
        if config.contract.quarantine_action not in ("fail_batch", "warn"):
            report.findings.append(LintFinding(
                level="ERROR",
                rule="INVALID_CONTRACT_ACTION",
                message=f"Contract quarantine_action '{config.contract.quarantine_action}' must be 'fail_batch' or 'warn'.",
                suggestion="Set quarantine_action to 'fail_batch' for strict SLA enforcement or 'warn' for passive monitoring.",
                target="contract.quarantine_action"
            ))

    return report

def lint_file_or_directory(path: Union[str, Path]) -> Tuple[bool, List[LintReport]]:
    """
    Lints a single YAML config file or all YAML files in a directory.
    Returns (all_passed, reports).
    """
    from lakehouse_engine.core.config_loader import load_pipeline_config

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Configuration target path does not exist: {p.resolve()}")

    files_to_lint: List[Path] = []
    if p.is_file():
        files_to_lint.append(p)
    else:
        files_to_lint.extend(sorted(p.rglob("*.yml")))
        files_to_lint.extend(sorted(p.rglob("*.yaml")))

    reports: List[LintReport] = []
    all_passed = True

    for yf in files_to_lint:
        try:
            cfg = load_pipeline_config(yf, allow_unresolved_secrets=True)
            report = lint_pipeline_config(cfg, file_path=str(yf))
            reports.append(report)
            if not report.is_valid:
                all_passed = False
        except Exception as e:
            err_report = LintReport(
                entity_name=yf.stem,
                file_path=str(yf),
                findings=[
                    LintFinding(
                        level="ERROR",
                        rule="CONFIG_PARSING_FAILURE",
                        message=f"Failed to load or parse configuration: {str(e)}",
                        suggestion="Verify YAML syntax and schema conformance.",
                        target=str(yf)
                    )
                ]
            )
            reports.append(err_report)
            all_passed = False

    return all_passed, reports

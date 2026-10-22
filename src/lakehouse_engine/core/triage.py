import re
import json
import difflib
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from pathlib import Path
import yaml

from lakehouse_engine.models.config import EntityPipelineConfig

logger = logging.getLogger(__name__)

@dataclass
class TriageFinding:
    dlq_reason: str
    failure_count: int
    percentage: float
    root_cause: str
    impact_assessment: str
    recommended_action: str
    suggested_yaml_patch: Optional[Dict[str, Any]] = None
    patch_description: Optional[str] = None

@dataclass
class TriageReport:
    entity_name: str
    dlq_table: str
    total_quarantined_records: int
    findings: List[TriageFinding]
    remediation_summary: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_name": self.entity_name,
            "dlq_table": self.dlq_table,
            "total_quarantined_records": self.total_quarantined_records,
            "remediation_summary": self.remediation_summary,
            "findings": [asdict(f) for f in self.findings]
        }

    def format_console_report(self) -> str:
        lines = [
            "=" * 80,
            f" 🩺 LAKEHOUSE ENGINE: AUTONOMOUS DLQ TRIAGE & SELF-HEALING REPORT",
            "=" * 80,
            f" Entity:              {self.entity_name}",
            f" Target DLQ Table:    {self.dlq_table}",
            f" Quarantined Rows:    {self.total_quarantined_records}",
            "-" * 80,
        ]

        if not self.findings:
            lines.append("  ✅ No quarantined records detected. Pipeline health is 100% clean.")
            lines.append("=" * 80)
            return "\n".join(lines)

        for idx, finding in enumerate(self.findings, 1):
            lines.append(f"\n[Finding #{idx}] Reason: {finding.dlq_reason}")
            lines.append(f"  • Impact:         {finding.failure_count} row(s) ({finding.percentage:.1f}%)")
            lines.append(f"  • Root Cause:     {finding.root_cause}")
            lines.append(f"  • Assessment:     {finding.impact_assessment}")
            lines.append(f"  • Recommendation: {finding.recommended_action}")
            if finding.patch_description:
                lines.append(f"  • Suggested Fix:  {finding.patch_description}")

        lines.append("\n" + "-" * 80)
        lines.append(f" Remediation Summary: {self.remediation_summary}")
        lines.append("=" * 80)
        return "\n".join(lines)


def _find_key_candidates(
    payload: Dict[str, Any],
    target_key: str,
    prefix: str = "payload",
    excluded_exprs: Optional[set] = None
) -> List[Tuple[str, Any]]:
    """Recursively searches a payload dict for candidate keys, excluding fields already claimed by other columns."""
    candidates = []
    target_clean = re.sub(r"[_\-]", "", target_key.lower())
    excluded = excluded_exprs or set()

    def _traverse(curr: Any, curr_path: str, raw_key: str):
        if not isinstance(curr, dict):
            return
        for k, v in curr.items():
            new_path = f"{curr_path}.{k}"
            # Skip attributes already claimed by another column in the schema
            if any(k in ex for ex in excluded):
                continue
            k_clean = re.sub(r"[_\-]", "", k.lower())
            is_match = (
                k_clean == target_clean
                or target_clean in k_clean
                or k_clean in target_clean
                or k_clean in ["id", "uuid", "pk", "key"]
                or (target_key.lower().endswith("id") and k.lower().endswith("id") and (target_clean.startswith(k_clean.replace("id", "")) or k_clean.startswith(target_clean.replace("id", ""))))
            )
            if is_match:
                candidates.append((new_path, v))
            if isinstance(v, dict):
                _traverse(v, new_path, k)

    _traverse(payload, prefix, "")
    return candidates


def analyze_quarantined_records(
    config: EntityPipelineConfig,
    records: List[Dict[str, Any]]
) -> TriageReport:
    """
    Analyzes quarantined records from a DLQ table or sample export.
    Clusters by failure reason and autonomously derives root cause and YAML fix.
    """
    total_records = len(records)
    if total_records == 0:
        return TriageReport(
            entity_name=config.entity_name,
            dlq_table=config.gold.dlq_fqdn,
            total_quarantined_records=0,
            findings=[],
            remediation_summary="All data passing validation. No triage required."
        )

    # Group records by reason
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        reason = r.get("_dlq_reason", "UNKNOWN_ERROR")
        grouped.setdefault(reason, []).append(r)

    findings: List[TriageFinding] = []

    for reason, fail_rows in grouped.items():
        count = len(fail_rows)
        pct = (count / total_records) * 100.0

        # Case 1: Missing primary key constraint
        if "MISSING_OR_EMPTY_KEY:" in reason:
            key_name = reason.split("MISSING_OR_EMPTY_KEY:")[1].split(",")[0].strip()
            
            # Inspect rescued payloads
            candidates_found: Dict[str, int] = {}
            for row in fail_rows[:20]:
                payload = row.get("_rescued_payload") or row.get("payload") or {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                excluded_claimed = {f.expr for f in config.gold.fields if f.name != key_name}
                for split in config.pii_splits:
                    excluded_claimed.update({f.expr for f in split.fields})
                for path, val in _find_key_candidates(payload, key_name, excluded_exprs=excluded_claimed):
                    candidates_found[path] = candidates_found.get(path, 0) + 1

            if candidates_found:
                top_candidate = max(candidates_found.items(), key=lambda x: x[1])[0]
                root_cause = f"Upstream source altered primary key naming or path for '{key_name}'. Discovered active candidate: '{top_candidate}'."
                impact = f"{count} records diverted from Gold table because '{key_name}' evaluated to NULL."
                action = f"Update '{key_name}' extraction to coalesce existing expr with discovered path '{top_candidate}'."
                
                # Find matching field in config
                existing_field = next((f for f in config.gold.fields if f.name == key_name), None)
                existing_expr = existing_field.expr if existing_field else f"payload.{key_name}"
                new_expr = f"coalesce({existing_expr}, {top_candidate})"
                
                findings.append(TriageFinding(
                    dlq_reason=reason,
                    failure_count=count,
                    percentage=pct,
                    root_cause=root_cause,
                    impact_assessment=impact,
                    recommended_action=action,
                    suggested_yaml_patch={"field": key_name, "new_expr": new_expr},
                    patch_description=f"Change field '{key_name}' expr from '{existing_expr}' -> '{new_expr}'"
                ))
            else:
                findings.append(TriageFinding(
                    dlq_reason=reason,
                    failure_count=count,
                    percentage=pct,
                    root_cause=f"Primary key '{key_name}' is genuinely absent in upstream JSON payload.",
                    impact_assessment=f"{count} records lack identity and cannot be merged into dimension/fact.",
                    recommended_action=f"Configure source 'stream_filter_field' to ignore null events or drop unidentifiable records.",
                    patch_description="Add stream_filter_field or keep in DLQ as non-billable dropped event."
                ))

        # Case 2: Data Quality Expectation Violation
        elif "FAILED_EXPECTATION:" in reason:
            exp_match = re.search(r"FAILED_EXPECTATION:([^\s]+)", reason)
            exp_name = exp_match.group(1) if exp_match else "unknown_rule"
            
            # Find the expectation definition
            matched_exp = next((e for e in config.gold.expectations if e.name == exp_name), None)
            expr_str = matched_exp.expr if matched_exp else reason
            
            # Check for categorical IN (...) violation
            in_match = re.search(r"(\w+)\s+IN\s*\(([^)]+)\)", expr_str, re.IGNORECASE)
            if in_match:
                col_name = in_match.group(1)
                allowed_vals = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
                
                # Scan violating values from payloads
                violating_vals = set()
                for row in fail_rows[:30]:
                    val = row.get(col_name)
                    if val is None:
                        payload = row.get("_rescued_payload") or row.get("payload") or {}
                        if isinstance(payload, str):
                            try: payload = json.loads(payload)
                            except Exception: payload = {}
                        val = payload.get(col_name)
                    if val and str(val) not in allowed_vals:
                        violating_vals.add(str(val))

                new_vals_str = ", ".join(f"'{v}'" for v in sorted(violating_vals))
                all_vals_str = ", ".join(f"'{v}'" for v in sorted(set(allowed_vals).union(violating_vals)))
                new_rule_expr = f"{col_name} IN ({all_vals_str})"

                root_cause = f"Upstream business logic introduced new categorical value(s): [{new_vals_str}] not present in expectation '{exp_name}'."
                impact = f"{count} valid business records quarantined due to strict expectation rules."
                action = f"Expand expectation '{exp_name}' allowed set to include {new_vals_str}."

                findings.append(TriageFinding(
                    dlq_reason=reason,
                    failure_count=count,
                    percentage=pct,
                    root_cause=root_cause,
                    impact_assessment=impact,
                    recommended_action=action,
                    suggested_yaml_patch={"expectation": exp_name, "new_expr": new_rule_expr},
                    patch_description=f"Update expectation '{exp_name}' expr to: {new_rule_expr}"
                ))
            else:
                findings.append(TriageFinding(
                    dlq_reason=reason,
                    failure_count=count,
                    percentage=pct,
                    root_cause=f"Expectation rule '{exp_name}' ({expr_str}) failed on {count} record(s).",
                    impact_assessment=f"{count} records quarantined due to business assertion failure.",
                    recommended_action="Inspect business semantics with domain owners or update validation thresholds."
                ))

        else:
            findings.append(TriageFinding(
                dlq_reason=reason,
                failure_count=count,
                percentage=pct,
                root_cause=f"Quarantined under reason: {reason}",
                impact_assessment=f"{count} record(s) quarantined.",
                recommended_action="Inspect raw payload JSON structure."
            ))

    remediation_summary = f"Identified {len(findings)} distinct failure cluster(s) across {total_records} records. Auto-remediation available for {sum(1 for f in findings if f.suggested_yaml_patch is not None)} issue(s)."

    return TriageReport(
        entity_name=config.entity_name,
        dlq_table=config.gold.dlq_fqdn,
        total_quarantined_records=total_records,
        findings=findings,
        remediation_summary=remediation_summary
    )


def apply_triage_patches_to_yaml(
    yaml_path: str,
    report: TriageReport
) -> Tuple[bool, str]:
    """
    Applies recommended triage patches directly to the entity YAML configuration.
    Returns (success, diff_string).
    """
    p = Path(yaml_path)
    if not p.exists():
        return False, f"Config file not found: {yaml_path}"

    original_text = p.read_text()
    data = yaml.safe_load(original_text)

    patches_applied = 0
    for finding in report.findings:
        if not finding.suggested_yaml_patch:
            continue

        patch = finding.suggested_yaml_patch
        # Patch Field expr
        if "field" in patch:
            field_name = patch["field"]
            new_expr = patch["new_expr"]
            for f in data.get("gold", {}).get("fields", []):
                if f.get("name") == field_name:
                    f["expr"] = new_expr
                    patches_applied += 1

        # Patch Expectation expr
        if "expectation" in patch:
            exp_name = patch["expectation"]
            new_expr = patch["new_expr"]
            for e in data.get("gold", {}).get("expectations", []):
                if e.get("name") == exp_name:
                    e["expr"] = new_expr
                    patches_applied += 1

    if patches_applied == 0:
        return False, "No actionable patches found in triage report."

    new_text = yaml.dump(data, sort_keys=False)
    diff = difflib.unified_diff(
        original_text.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        fromfile=f"a/{p.name}",
        tofile=f"b/{p.name}"
    )
    diff_str = "".join(diff)

    p.write_text(new_text)
    return True, diff_str

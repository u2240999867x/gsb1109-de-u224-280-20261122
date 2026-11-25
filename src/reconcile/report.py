from __future__ import annotations

import json
from typing import Any

from .engine import ReconciliationReport


def render_json(report: ReconciliationReport) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=False)


def render_markdown(report: ReconciliationReport) -> str:
    data = report.to_dict()
    lines: list[str] = [f"# Reconciliation report: {data['batch_id']}", ""]
    lines.append(f"Status: `{data['status']}`")
    summary = data["summary"]
    lines.append(
        "Summary: "
        f"{summary['unique_source_rows']} unique, "
        f"{summary['ambiguous_rows']} ambiguous, "
        f"{summary['unresolved_rows']} unresolved, "
        f"{summary['issues']} issue(s)."
    )
    lines.append("")

    if data["issues"]:
        lines.extend(["## Contradictions", ""])
        for issue in data["issues"]:
            location = _location(issue)
            lines.append(
                f"- `{issue['code']}` {issue['message']} "
                f"Location: station `{issue['station_id']}`, "
                f"observation `{issue['observation_id']}`{location}."
            )
        lines.append("")

    lines.extend(["## Warehouse observations", ""])
    for result in data["warehouse_results"]:
        lines.append(f"### `{result['warehouse_row']}`")
        if result["unique_source"]:
            lines.append(
                f"Unique original event: `{result['unique_original_event']}`."
            )
        elif result["ambiguous"]:
            candidates = ", ".join(f"`{source}`" for source in result["candidate_sources"])
            lines.append(f"Ambiguous candidate sources: {candidates}.")
            lines.append("No ordering or sort rule is used to select one.")
        else:
            lines.append("No station-valid source chain.")

        for chain_index, chain in enumerate(result["chains"], start=1):
            lines.append("")
            lines.append(f"Chain {chain_index}: `{' -> '.join(chain['nodes'])}`")
            if chain["observation_ids"]:
                lines.append(
                    "- Observations: "
                    + ", ".join(f"`{item}`" for item in chain["observation_ids"])
                )
            if chain["resend_observation_ids"]:
                lines.append(
                    "- Resend observations on same chain: "
                    + ", ".join(
                        f"`{item}`" for item in chain["resend_observation_ids"]
                    )
                    + f" (group `{', '.join(chain['resend_groups'])}`)"
                )
            if chain["matching_ids"]:
                lines.append(
                    "- Station matchings: "
                    + ", ".join(f"`{item}`" for item in chain["matching_ids"])
                )

        if result["ambiguous"]:
            lines.append("")
            lines.append("Verifiable constraints still satisfied by every candidate:")
            for constraint in result["verifiable_constraints"]:
                detail = _constraint_detail(constraint)
                suffix = f" ({detail})" if detail else ""
                lines.append(
                    f"- `{constraint['type']}` @ `{constraint['scope']}`{suffix}"
                )
        lines.append("")

    lines.extend(["## Observation log", ""])
    for observation in data["observations"]:
        location = _location(observation)
        flags = " [contradictory]" if observation["contradictory"] else ""
        lines.append(
            f"- `{observation['id']}` ({observation['kind']}) "
            f"station=`{observation['station_id']}`{location}{flags}"
        )
    lines.append("")

    lines.extend(["## Station matchings", ""])
    for station in data["stations"]:
        lines.append(
            f"- `{station['id']}`: {station['matching_count']} compliant matching(s)"
        )
        for matching in station["matchings"]:
            pairs = ", ".join(
                f"`{pair['input']} -> {pair['output']}`"
                for pair in matching["pairs"]
            )
            lines.append(f"  - `{matching['id']}`: {pairs}")
    lines.append("")
    return "\n".join(lines)


def _location(record: dict[str, Any]) -> str:
    if record.get("source_ref") and record.get("line") is not None:
        return f" ({record['source_ref']}:{record['line']})"
    if record.get("source_ref"):
        return f" ({record['source_ref']})"
    if record.get("line") is not None:
        return f" (line {record['line']})"
    return ""


def _constraint_detail(constraint: dict[str, Any]) -> str:
    if constraint["type"] in {
        "complete_input_count",
        "complete_output_count",
    }:
        return f"expected={constraint.get('expected')}"
    if constraint["type"] == "candidate_input_range":
        candidates = ", ".join(constraint.get("candidate_inputs", []))
        return f"candidates=[{candidates}]"
    if constraint["type"] == "resend_group_equivalence":
        return "outbound rows in one group map to one logical output"
    return ""

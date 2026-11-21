"""Output renderers: text, json, markdown."""

from __future__ import annotations

import json
from typing import Any

from .rules import CRITICAL, OK, WARN, StreamReport

_MARK = {CRITICAL: "CRIT", WARN: "WARN", OK: "ok"}


def _counts(reports: list[StreamReport]) -> dict[str, int]:
    out = {CRITICAL: 0, WARN: 0, OK: 0}
    for report in reports:
        out[report.severity] += 1
    return out


def render_text(reports: list[StreamReport], show_clean: bool = False) -> str:
    lines: list[str] = []
    for report in sorted(reports, key=lambda r: (r.region, r.name)):
        if report.severity == OK and not show_clean:
            continue
        header = (
            f"[{_MARK[report.severity]}] {report.region}/{report.name} "
            f"({report.mode}, {report.open_shards} open shard(s), "
            f"retention {report.retention_hours}h)"
        )
        lines.append(header)
        for finding in report.findings:
            lines.append(f"    {_MARK[finding.severity]:>4}  {finding.rule}: {finding.message}")
        lines.append("")
    counts = _counts(reports)
    lines.append(
        f"{len(reports)} stream(s): {counts[CRITICAL]} critical, "
        f"{counts[WARN]} warn, {counts[OK]} clean"
    )
    return "\n".join(lines)


def render_markdown(reports: list[StreamReport], show_clean: bool = False) -> str:
    counts = _counts(reports)
    lines = [
        "# Kinesis Data Streams audit",
        "",
        f"{len(reports)} stream(s) audited: **{counts[CRITICAL]} critical**, "
        f"{counts[WARN]} warn, {counts[OK]} clean.",
        "",
        "| Region | Stream | Mode | Shards | Retention | Severity | Findings |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for report in sorted(reports, key=lambda r: (r.region, r.name)):
        if report.severity == OK and not show_clean:
            continue
        lines.append(
            f"| {report.region} | `{report.name}` | {report.mode} | {report.open_shards} "
            f"| {report.retention_hours}h | {report.severity} | {len(report.findings)} |"
        )
    for report in sorted(reports, key=lambda r: (r.region, r.name)):
        if report.severity == OK and not show_clean:
            continue
        lines += ["", f"## {report.region}/{report.name}", ""]
        for finding in report.findings:
            lines.append(f"- **{finding.severity}** `{finding.rule}` — {finding.message}")
    return "\n".join(lines) + "\n"


def render_json(reports: list[StreamReport], payload: dict[str, Any] | None = None) -> str:
    counts = _counts(reports)
    body = {
        "generated_at": (payload or {}).get("generated_at"),
        "summary": {
            "streams": len(reports),
            "critical": counts[CRITICAL],
            "warn": counts[WARN],
            "ok": counts[OK],
        },
        "streams": [report.as_dict() for report in reports],
    }
    return json.dumps(body, indent=2, sort_keys=False)


def render(
    fmt: str,
    reports: list[StreamReport],
    payload: dict[str, Any] | None = None,
    show_clean: bool = False,
) -> str:
    if fmt == "json":
        return render_json(reports, payload)
    if fmt == "markdown":
        return render_markdown(reports, show_clean)
    return render_text(reports, show_clean)

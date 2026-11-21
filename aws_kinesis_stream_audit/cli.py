"""Command-line entry point.

Exit codes: 0 clean (or below --fail-on), 1 findings at or above --fail-on,
2 a usage or input error. A bad file is a message on stderr, never a traceback.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from typing import Any

from . import __version__
from .collect import CollectionError, collect, load_payload
from .report import render
from .rules import CRITICAL, OK, WARN, RulesError, evaluate, merge_rules, worst_severity

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_USAGE = 2

_SEVERITY_ORDER = {OK: 0, WARN: 1, CRITICAL: 2}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aws-kinesis-stream-audit",
        description=(
            "Audit Amazon Kinesis Data Streams configuration: retention, encryption, "
            "shard capacity against declared throughput, enhanced fan-out consumers "
            "and shard-level metrics."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    source = parser.add_argument_group("source")
    source.add_argument("--region", help="single region to audit")
    source.add_argument("--regions", help="comma-separated regions to audit")
    source.add_argument("--profile", help="AWS profile name")
    source.add_argument("--from-json", metavar="PATH", help="replay a saved payload, no AWS calls")
    source.add_argument("--dump-json", metavar="PATH", help="write the collected payload here")

    thresholds = parser.add_argument_group("thresholds")
    thresholds.add_argument(
        "--min-retention-hours", type=int, help="warn below this retention (default 24)"
    )
    thresholds.add_argument(
        "--critical-tag",
        help="tag key (or key=value) marking a source-of-record stream (default source-of-record)",
    )
    thresholds.add_argument(
        "--overprovision-factor",
        type=float,
        help="warn when shard capacity exceeds the hint by more than this (default 2.0)",
    )
    thresholds.add_argument(
        "--stale-consumer-days",
        type=int,
        help="warn on enhanced fan-out consumers older than this (default 90)",
    )
    thresholds.add_argument(
        "--require-tags", action="store_true", help="warn on streams with no tags"
    )
    thresholds.add_argument(
        "--rules", metavar="PATH", help="YAML or JSON file overriding any threshold"
    )
    thresholds.add_argument(
        "--throughput-hints",
        metavar="PATH",
        help="YAML or JSON map of stream name to {mb_per_s, records_per_s}",
    )

    output = parser.add_argument_group("output")
    output.add_argument(
        "--format", choices=("text", "json", "markdown"), default="text", help="output format"
    )
    output.add_argument(
        "--fail-on",
        choices=("warn", "critical", "none"),
        default="critical",
        help="severity that makes the exit code 1 (default critical)",
    )
    output.add_argument(
        "--show-clean", action="store_true", help="include streams with no findings"
    )
    return parser


def _load_structured(path: str, label: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError as exc:
        raise RulesError(f"{label} {path}: no such file") from exc
    except OSError as exc:
        raise RulesError(f"{label} {path}: {exc}") from exc
    data: Any
    if path.endswith((".yml", ".yaml")):
        try:
            import yaml
        except ImportError as exc:
            raise RulesError(
                f"{label} {path} is YAML but PyYAML is not installed; use JSON instead"
            ) from exc
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise RulesError(f"{label} {path}: invalid YAML ({exc})") from exc
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RulesError(
                f"{label} {path}: invalid JSON ({exc.msg} at line {exc.lineno})"
            ) from exc
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise RulesError(f"{label} {path}: expected a mapping at the top level")
    return data


def _normalize_hints(raw: dict[str, Any], path: str) -> dict[str, dict[str, float]]:
    hints: dict[str, dict[str, float]] = {}
    for name, value in raw.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            hints[name] = {"mb_per_s": float(value), "records_per_s": 0}
            continue
        if not isinstance(value, dict):
            raise RulesError(
                f"throughput hints {path}: {name!r} must be a number or a mapping"
            )
        try:
            hints[name] = {
                "mb_per_s": float(value.get("mb_per_s", 0) or 0),
                "records_per_s": int(value.get("records_per_s", 0) or 0),
            }
        except (TypeError, ValueError) as exc:
            raise RulesError(f"throughput hints {path}: {name!r} has a non-numeric value") from exc
    return hints


def _regions(args: argparse.Namespace) -> list[str]:
    names: list[str] = []
    if args.regions:
        names.extend(r.strip() for r in args.regions.split(",") if r.strip())
    if args.region:
        names.append(args.region.strip())
    seen: set[str] = set()
    ordered = []
    for name in names:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def _overrides(args: argparse.Namespace, file_rules: dict[str, Any]) -> dict[str, Any]:
    overrides = dict(file_rules)
    if args.min_retention_hours is not None:
        overrides["min_retention_hours"] = args.min_retention_hours
    if args.critical_tag is not None:
        overrides["critical_tag"] = args.critical_tag
    if args.overprovision_factor is not None:
        overrides["overprovision_factor"] = args.overprovision_factor
    if args.stale_consumer_days is not None:
        overrides["stale_consumer_days"] = args.stale_consumer_days
    if args.require_tags:
        overrides["require_tags"] = True
    return overrides


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        file_rules = _load_structured(args.rules, "rules") if args.rules else {}
        rules = merge_rules(_overrides(args, file_rules))
        hints: dict[str, Any] = {}
        if args.throughput_hints:
            hints = _normalize_hints(
                _load_structured(args.throughput_hints, "throughput hints"),
                args.throughput_hints,
            )
    except RulesError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    if args.from_json:
        if _regions(args):
            print(
                "error: --from-json replays a saved payload; drop --region/--regions",
                file=sys.stderr,
            )
            return EXIT_USAGE
        try:
            payload = load_payload(args.from_json)
        except CollectionError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_USAGE
    else:
        regions = _regions(args)
        if not regions:
            print("error: give --region, --regions or --from-json", file=sys.stderr)
            return EXIT_USAGE
        try:
            payload = collect(regions, args.profile)
        except CollectionError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_USAGE

    if args.dump_json:
        try:
            with open(args.dump_json, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except OSError as exc:
            print(f"error: --dump-json {args.dump_json}: {exc}", file=sys.stderr)
            return EXIT_USAGE

    reports = evaluate(payload, rules, hints, dt.datetime.now(dt.UTC))
    print(render(args.format, reports, payload, args.show_clean))

    if args.fail_on == "none":
        return EXIT_OK
    threshold = _SEVERITY_ORDER[args.fail_on]
    if _SEVERITY_ORDER[worst_severity(reports)] >= threshold:
        return EXIT_FINDINGS
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

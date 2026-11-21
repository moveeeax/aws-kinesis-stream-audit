"""The rule engine.

Input is the plain-dict payload from :mod:`collect`. Output is one record per
stream with a list of findings and a rolled-up severity. No AWS calls, no
clock reads except ``now``, which the caller passes in so tests are stable.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from . import pricing

WARN = "warn"
CRITICAL = "critical"
OK = "ok"

_ORDER = {OK: 0, WARN: 1, CRITICAL: 2}

AWS_OWNED_KEY_ALIASES = ("alias/aws/kinesis",)

REQUIRED_SHARD_METRICS = ("IncomingBytes", "WriteProvisionedThroughputExceeded")

DEFAULT_RULES: dict[str, Any] = {
    "min_retention_hours": 24,
    "critical_retention_hours": 168,
    "critical_tag": "source-of-record",
    "overprovision_factor": 2.0,
    "stale_consumer_days": 90,
    "max_efo_consumers": 20,
    "hash_range_skew_factor": 2.0,
    "require_tags": False,
    "shard_hour_usd": pricing.SHARD_HOUR_USD,
    "efo_consumer_shard_hour_usd": pricing.EFO_CONSUMER_SHARD_HOUR_USD,
}

_TRUTHY = {"1", "true", "yes", "y", "on"}


class RulesError(Exception):
    """Raised when a rules or throughput-hints file cannot be used."""


@dataclass
class Finding:
    rule: str
    severity: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"rule": self.rule, "severity": self.severity, "message": self.message}


@dataclass
class StreamReport:
    region: str
    name: str
    mode: str
    status: str
    open_shards: int
    retention_hours: int | None
    findings: list[Finding] = field(default_factory=list)

    @property
    def severity(self) -> str:
        worst = OK
        for finding in self.findings:
            if _ORDER[finding.severity] > _ORDER[worst]:
                worst = finding.severity
        return worst

    def as_dict(self) -> dict[str, Any]:
        return {
            "region": self.region,
            "stream": self.name,
            "mode": self.mode,
            "status": self.status,
            "open_shards": self.open_shards,
            "retention_hours": self.retention_hours,
            "severity": self.severity,
            "findings": [f.as_dict() for f in self.findings],
        }


def merge_rules(overrides: dict[str, Any] | None) -> dict[str, Any]:
    rules = dict(DEFAULT_RULES)
    if not overrides:
        return rules
    unknown = sorted(set(overrides) - set(DEFAULT_RULES))
    if unknown:
        raise RulesError(f"unknown rule key(s): {', '.join(unknown)}")
    for key, value in overrides.items():
        expected = type(DEFAULT_RULES[key])
        if expected is bool and not isinstance(value, bool):
            raise RulesError(f"rule {key!r} must be a boolean")
        if expected in (int, float) and isinstance(value, bool):
            raise RulesError(f"rule {key!r} must be a number")
        if expected in (int, float) and not isinstance(value, (int, float)):
            raise RulesError(f"rule {key!r} must be a number")
        if expected is str and not isinstance(value, str):
            raise RulesError(f"rule {key!r} must be a string")
        rules[key] = value
    return rules


def _open_shards(stream: dict) -> list[dict]:
    shards = stream.get("shards") or []
    return [s for s in shards if not s.get("closed")]


def _open_shard_count(stream: dict) -> int:
    declared = stream.get("open_shard_count")
    if isinstance(declared, int) and declared > 0:
        return declared
    return len(_open_shards(stream))


def _hint_for(stream: dict, hints: dict[str, Any]) -> dict[str, float] | None:
    name = stream.get("name")
    if name in hints:
        return hints[name]
    tags = stream.get("tags") or {}
    raw = tags.get("kinesis-throughput-mbps")
    if raw is None:
        return None
    try:
        return {"mb_per_s": float(raw), "records_per_s": 0}
    except (TypeError, ValueError):
        return None


def _is_critical_tagged(stream: dict, key: str) -> bool:
    if not key:
        return False
    tags = stream.get("tags") or {}
    if "=" in key:
        wanted_key, wanted_value = key.split("=", 1)
        return tags.get(wanted_key) == wanted_value
    if key not in tags:
        return False
    value = str(tags[key]).strip().lower()
    return value in _TRUTHY or value == ""


def _parse_ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed


def _retention_findings(stream: dict, rules: dict, shards: int) -> list[Finding]:
    out: list[Finding] = []
    hours = stream.get("retention_hours")
    if not isinstance(hours, int):
        return out
    critical_hours = int(rules["critical_retention_hours"])
    if _is_critical_tagged(stream, str(rules["critical_tag"])) and hours < critical_hours:
        out.append(
            Finding(
                "retention.source_of_record",
                CRITICAL,
                f"tagged {rules['critical_tag']!r} but retention is {hours}h, "
                f"under the {critical_hours}h this stream's own tag claims it needs",
            )
        )
        return out
    if hours == 24:
        message = (
            "retention is the 24h default: a consumer outage longer than a day is "
            "unrecoverable, there is nothing to replay"
        )
        # Extended retention is billed per shard-hour, which only means a
        # predictable number on a provisioned stream.
        if (stream.get("mode") or "PROVISIONED").upper() == "PROVISIONED":
            extra = pricing.extended_retention_cost_monthly(shards)
            message += (
                f". 7 days costs about {pricing.usd(extra)}/month at {shards} shard(s)"
            )
        out.append(Finding("retention.default_24h", WARN, message))
        return out
    minimum = int(rules["min_retention_hours"])
    if hours < minimum:
        out.append(
            Finding(
                "retention.below_minimum",
                WARN,
                f"retention is {hours}h, under the required {minimum}h",
            )
        )
    return out


def _encryption_findings(stream: dict) -> list[Finding]:
    encryption = (stream.get("encryption_type") or "NONE").upper()
    if encryption == "NONE":
        return [
            Finding(
                "encryption.disabled",
                CRITICAL,
                "server-side encryption is off: records sit unencrypted at rest "
                "for the whole retention window",
            )
        ]
    key = str(stream.get("key_id") or "")
    if key in AWS_OWNED_KEY_ALIASES or key.endswith("/aws/kinesis"):
        return [
            Finding(
                "encryption.aws_owned_key",
                WARN,
                f"encrypted with the AWS-managed key {key!r}, not a customer-managed "
                "CMK: no key policy of yours applies and rotation is not yours to prove",
            )
        ]
    return []


def _capacity_findings(stream: dict, rules: dict, shards: int, hints: dict) -> list[Finding]:
    out: list[Finding] = []
    hint = _hint_for(stream, hints)
    mode = (stream.get("mode") or "PROVISIONED").upper()
    capacity = pricing.capacity_for(shards)
    if mode == "ON_DEMAND":
        if hint and pricing.shards_needed(
            float(hint.get("mb_per_s", 0) or 0), int(hint.get("records_per_s", 0) or 0)
        ) <= 1:
            out.append(
                Finding(
                    "capacity.on_demand_oversized",
                    WARN,
                    f"on-demand but the declared need of {hint.get('mb_per_s', 0)} MB/s fits "
                    "in a single shard; provisioned mode is cheaper at this size",
                )
            )
        return out
    if hint is None:
        return out
    need_mb = float(hint.get("mb_per_s", 0) or 0)
    need_records = int(hint.get("records_per_s", 0) or 0)
    if need_mb > capacity.ingress_mb_s:
        shortfall = round(need_mb - capacity.ingress_mb_s, 3)
        out.append(
            Finding(
                "capacity.under_provisioned",
                CRITICAL,
                f"{shards} open shard(s) carry {capacity.ingress_mb_s} MB/s but the stream "
                f"declares {need_mb} MB/s: a {shortfall} MB/s shortfall, so producers eat "
                "ProvisionedThroughputExceeded and the SDK retries hide it",
            )
        )
        return out
    if need_records > capacity.ingress_records_s:
        shortfall = need_records - capacity.ingress_records_s
        out.append(
            Finding(
                "capacity.under_provisioned_records",
                CRITICAL,
                f"{shards} open shard(s) carry {capacity.ingress_records_s} records/s but the "
                f"stream declares {need_records}/s: a {shortfall} records/s shortfall",
            )
        )
        return out
    factor = float(rules["overprovision_factor"])
    if need_mb > 0 and capacity.ingress_mb_s > need_mb * factor:
        needed = pricing.shards_needed(need_mb, need_records)
        excess = shards - needed
        if excess > 0:
            cost = pricing.shard_cost_monthly(excess, float(rules["shard_hour_usd"]))
            out.append(
                Finding(
                    "capacity.over_provisioned",
                    WARN,
                    f"{shards} open shard(s) for a declared {need_mb} MB/s that needs "
                    f"{needed}: {excess} excess shard(s) at about {pricing.usd(cost)}/month",
                )
            )
    return out


def _consumer_findings(
    stream: dict, rules: dict, shards: int, now: dt.datetime
) -> list[Finding]:
    out: list[Finding] = []
    consumers = stream.get("consumers") or []
    active = [c for c in consumers if (c.get("status") or "").upper() == "ACTIVE"]
    if not consumers:
        out.append(
            Finding(
                "consumers.none_registered",
                WARN,
                "no registered consumer at all: either everything reads with GetRecords "
                "or the stream is abandoned and still billing shard-hours",
            )
        )
        return out
    limit = int(rules["max_efo_consumers"])
    if len(consumers) > limit:
        out.append(
            Finding(
                "consumers.over_fanout_limit",
                CRITICAL,
                f"{len(consumers)} registered consumers, over the per-stream enhanced "
                f"fan-out limit of {limit}: new registrations will be rejected",
            )
        )
    stale_days = int(rules["stale_consumer_days"])
    efo_price = float(rules["efo_consumer_shard_hour_usd"])
    for consumer in consumers:
        status = (consumer.get("status") or "UNKNOWN").upper()
        name = consumer.get("name") or "<unnamed>"
        if status != "ACTIVE":
            out.append(
                Finding(
                    "consumers.not_active",
                    WARN,
                    f"consumer {name!r} is {status}, not ACTIVE: it is registered, "
                    "possibly billed, and reading nothing",
                )
            )
            continue
        created = _parse_ts(consumer.get("creation_timestamp"))
        if created is None:
            continue
        age_days = (now - created).days
        if age_days >= stale_days:
            cost = pricing.efo_cost_monthly(shards, 1, efo_price)
            out.append(
                Finding(
                    "consumers.stale_efo",
                    WARN,
                    f"enhanced fan-out consumer {name!r} registered {age_days} days ago and "
                    f"never re-registered: {pricing.usd(cost)}/month at {shards} shard(s) "
                    "if nothing reads it",
                )
            )
    if not active:
        out.append(
            Finding(
                "consumers.none_active",
                WARN,
                "every registered consumer is in a non-ACTIVE state: nothing is using "
                "enhanced fan-out on this stream",
            )
        )
    return out


def _observability_findings(stream: dict, rules: dict) -> list[Finding]:
    out: list[Finding] = []
    metrics = [m.upper() for m in (stream.get("shard_level_metrics") or [])]
    if "ALL" not in metrics:
        missing = [m for m in REQUIRED_SHARD_METRICS if m.upper() not in metrics]
        if missing:
            out.append(
                Finding(
                    "observability.shard_metrics_off",
                    WARN,
                    "shard-level metrics missing " + ", ".join(missing)
                    + ": a hot shard from a bad partition key cannot be found without them",
                )
            )
    open_shards = _open_shards(stream)
    if len(open_shards) > 1:
        widths = []
        for shard in open_shards:
            try:
                start = int(shard["starting_hash_key"])
                end = int(shard["ending_hash_key"])
            except (KeyError, TypeError, ValueError):
                widths = []
                break
            widths.append((end - start + 1, shard.get("shard_id")))
        if widths:
            widest = max(widths)
            narrowest = min(widths)
            factor = float(rules["hash_range_skew_factor"])
            if narrowest[0] > 0 and widest[0] > narrowest[0] * factor:
                ratio = round(widest[0] / narrowest[0], 1)
                out.append(
                    Finding(
                        "observability.uneven_hash_ranges",
                        WARN,
                        f"hash-key ranges are uneven after a manual reshard: {widest[1]} is "
                        f"{ratio}x wider than {narrowest[1]}, so an even key distribution "
                        "still lands unevenly across shards",
                    )
                )
    status = (stream.get("status") or "").upper()
    if status and status != "ACTIVE":
        out.append(
            Finding(
                "hygiene.not_active",
                WARN,
                f"stream status is {status}: a reshard or delete is in flight",
            )
        )
    if rules["require_tags"] and not (stream.get("tags") or {}):
        out.append(
            Finding(
                "hygiene.untagged",
                WARN,
                "no tags at all: no owner, no cost allocation, nobody to ask",
            )
        )
    return out


def evaluate_stream(
    stream: dict,
    region: str,
    rules: dict[str, Any],
    hints: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> StreamReport:
    hints = hints or {}
    now = now or dt.datetime.now(dt.UTC)
    shards = _open_shard_count(stream)
    report = StreamReport(
        region=region,
        name=stream.get("name") or "<unnamed>",
        mode=(stream.get("mode") or "PROVISIONED").upper(),
        status=stream.get("status") or "UNKNOWN",
        open_shards=shards,
        retention_hours=stream.get("retention_hours"),
    )
    report.findings.extend(_retention_findings(stream, rules, shards))
    report.findings.extend(_encryption_findings(stream))
    report.findings.extend(_capacity_findings(stream, rules, shards, hints))
    report.findings.extend(_consumer_findings(stream, rules, shards, now))
    report.findings.extend(_observability_findings(stream, rules))
    return report


def evaluate(
    payload: dict,
    rules: dict[str, Any] | None = None,
    hints: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
) -> list[StreamReport]:
    rules = rules or dict(DEFAULT_RULES)
    reports: list[StreamReport] = []
    for region in payload.get("regions") or []:
        name = region.get("region") or "unknown"
        for stream in region.get("streams") or []:
            reports.append(evaluate_stream(stream, name, rules, hints, now))
    return reports


def worst_severity(reports: list[StreamReport]) -> str:
    worst = OK
    for report in reports:
        if _ORDER[report.severity] > _ORDER[worst]:
            worst = report.severity
    return worst

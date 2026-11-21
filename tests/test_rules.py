import datetime as dt

import pytest

from aws_kinesis_stream_audit import pricing
from aws_kinesis_stream_audit.rules import (
    CRITICAL,
    DEFAULT_RULES,
    OK,
    WARN,
    RulesError,
    evaluate,
    evaluate_stream,
    merge_rules,
    worst_severity,
)

from .conftest import stream_named


def rules(**over):
    return merge_rules(over)


def findings(report, rule):
    return [f for f in report.findings if f.rule == rule]


def evaluate_one(payload, name, hints=None, now=None, **over):
    stream = stream_named(payload, name)
    return evaluate_stream(stream, "eu-central-1", rules(**over), hints or {}, now)


# --- retention ---------------------------------------------------------------


def test_default_retention_is_warned(prod, now):
    report = evaluate_one(prod, "telemetry-ondemand", now=now)
    hit = findings(report, "retention.default_24h")
    assert len(hit) == 1
    assert hit[0].severity == WARN
    assert "24h default" in hit[0].message
    # On-demand has no shard-hour bill to quote.
    assert "/month" not in hit[0].message


def test_default_retention_quotes_the_upgrade_cost_when_provisioned(prod, now):
    report = evaluate_one(prod, "orders-events", now=now)
    hit = findings(report, "retention.default_24h")
    assert pricing.usd(pricing.extended_retention_cost_monthly(2)) in hit[0].message


def test_source_of_record_tag_makes_short_retention_critical(prod, now):
    report = evaluate_one(prod, "billing-ledger", now=now)
    hit = findings(report, "retention.source_of_record")
    assert len(hit) == 1
    assert hit[0].severity == CRITICAL
    assert "48h" in hit[0].message
    assert report.severity == CRITICAL


def test_same_stream_without_the_tag_is_only_warned(prod, now):
    stream = stream_named(prod, "billing-ledger")
    stream["tags"].pop("source-of-record")
    stream["retention_hours"] = 24
    report = evaluate_stream(stream, "eu-central-1", rules(), {}, now)
    assert not findings(report, "retention.source_of_record")
    assert findings(report, "retention.default_24h")[0].severity == WARN


def test_retention_below_explicit_minimum(prod, now):
    report = evaluate_one(prod, "billing-ledger", now=now, critical_tag="unused-key",
                          min_retention_hours=168)
    hit = findings(report, "retention.below_minimum")
    assert hit and hit[0].severity == WARN
    assert "under the required 168h" in hit[0].message


# --- encryption --------------------------------------------------------------


def test_missing_encryption_is_critical(prod, now):
    report = evaluate_one(prod, "clickstream-raw", now=now)
    hit = findings(report, "encryption.disabled")
    assert len(hit) == 1
    assert hit[0].severity == CRITICAL


def test_aws_owned_key_is_warned(prod, now):
    report = evaluate_one(prod, "billing-ledger", now=now)
    hit = findings(report, "encryption.aws_owned_key")
    assert len(hit) == 1
    assert hit[0].severity == WARN
    assert "alias/aws/kinesis" in hit[0].message


def test_customer_managed_key_is_clean(prod, now):
    report = evaluate_one(prod, "audit-trail", now=now)
    assert not findings(report, "encryption.disabled")
    assert not findings(report, "encryption.aws_owned_key")


# --- capacity ----------------------------------------------------------------


def test_two_shards_against_a_five_mb_hint_is_critical(prod, hints, now):
    report = evaluate_one(prod, "orders-events", hints=hints, now=now)
    hit = findings(report, "capacity.under_provisioned")
    assert len(hit) == 1
    assert hit[0].severity == CRITICAL
    assert "3.0 MB/s shortfall" in hit[0].message


def test_record_rate_shortfall_is_reported(prod, now):
    stream = stream_named(prod, "orders-events")
    stream["tags"].pop("kinesis-throughput-mbps")
    hints = {"orders-events": {"mb_per_s": 1.0, "records_per_s": 5000}}
    report = evaluate_stream(stream, "eu-central-1", rules(), hints, now)
    hit = findings(report, "capacity.under_provisioned_records")
    assert hit and "3000 records/s shortfall" in hit[0].message


def test_overprovisioned_stream_reports_excess_cost(prod, hints, now):
    report = evaluate_one(prod, "clickstream-raw", hints=hints, now=now)
    hit = findings(report, "capacity.over_provisioned")
    assert len(hit) == 1
    assert hit[0].severity == WARN
    expected = pricing.usd(pricing.shard_cost_monthly(7))
    assert expected in hit[0].message


def test_on_demand_stream_that_fits_one_shard_is_warned(prod, hints, now):
    report = evaluate_one(prod, "telemetry-ondemand", hints=hints, now=now)
    hit = findings(report, "capacity.on_demand_oversized")
    assert len(hit) == 1
    assert hit[0].severity == WARN


def test_throughput_hint_can_come_from_a_tag(prod, now):
    report = evaluate_one(prod, "orders-events", hints={}, now=now)
    assert findings(report, "capacity.under_provisioned")


def test_no_hint_means_no_capacity_opinion(prod, now):
    stream = stream_named(prod, "orders-events")
    stream["tags"].pop("kinesis-throughput-mbps")
    report = evaluate_stream(stream, "eu-central-1", rules(), {}, now)
    assert not [f for f in report.findings if f.rule.startswith("capacity.")]


# --- consumers ---------------------------------------------------------------


def test_stream_without_consumers_is_warned(prod, now):
    report = evaluate_one(prod, "clickstream-raw", now=now)
    hit = findings(report, "consumers.none_registered")
    assert len(hit) == 1
    assert hit[0].severity == WARN


def test_stale_fanout_consumer_reports_its_monthly_cost(prod, now):
    report = evaluate_one(prod, "billing-ledger", now=now)
    hit = findings(report, "consumers.stale_efo")
    assert len(hit) == 1
    assert hit[0].severity == WARN
    assert pricing.usd(pricing.efo_cost_monthly(1, 1)) in hit[0].message


def test_fresh_consumer_is_not_stale(prod, now):
    report = evaluate_one(prod, "orders-events", now=now)
    assert not findings(report, "consumers.stale_efo")


def test_non_active_consumer_is_warned(prod, now):
    report = evaluate_one(prod, "reshard-leftover", now=now)
    assert findings(report, "consumers.not_active")[0].severity == WARN
    assert findings(report, "consumers.none_active")


def test_over_the_fanout_limit_is_critical(prod, now):
    stream = stream_named(prod, "orders-events")
    stream["consumers"] = [
        {"name": f"c{i}", "status": "ACTIVE", "creation_timestamp": "2026-11-01T00:00:00+00:00"}
        for i in range(21)
    ]
    report = evaluate_stream(stream, "eu-central-1", rules(), {}, now)
    hit = findings(report, "consumers.over_fanout_limit")
    assert hit and hit[0].severity == CRITICAL


# --- observability and hygiene ----------------------------------------------


def test_missing_shard_metrics_is_warned(prod, now):
    report = evaluate_one(prod, "clickstream-raw", now=now)
    hit = findings(report, "observability.shard_metrics_off")
    assert len(hit) == 1
    assert "IncomingBytes" in hit[0].message


def test_all_metrics_satisfies_the_rule(prod, now):
    report = evaluate_one(prod, "audit-trail", now=now)
    assert not findings(report, "observability.shard_metrics_off")


def test_uneven_hash_ranges_name_both_shards(prod, now):
    report = evaluate_one(prod, "reshard-leftover", now=now)
    hit = findings(report, "observability.uneven_hash_ranges")
    assert len(hit) == 1
    assert "shardId-000000000004" in hit[0].message
    assert "shardId-000000000005" in hit[0].message


def test_non_active_stream_status_is_warned(prod, now):
    report = evaluate_one(prod, "reshard-leftover", now=now)
    assert findings(report, "hygiene.not_active")[0].message.endswith(
        "a reshard or delete is in flight"
    )


def test_untagged_only_fires_with_require_tags(prod, now):
    clean = evaluate_one(prod, "reshard-leftover", now=now)
    assert not findings(clean, "hygiene.untagged")
    strict = evaluate_one(prod, "reshard-leftover", now=now, require_tags=True)
    assert findings(strict, "hygiene.untagged")


# --- closed shards and shard counting ---------------------------------------


def test_closed_shards_are_ignored_when_counting(prod, now):
    stream = stream_named(prod, "clickstream-raw")
    stream.pop("open_shard_count")
    report = evaluate_stream(stream, "eu-central-1", rules(), {}, now)
    assert report.open_shards == 8


# --- whole payload -----------------------------------------------------------


def test_clean_stream_has_no_findings(prod, hints, now):
    reports = {r.name: r for r in evaluate(prod, rules(), hints, now)}
    assert reports["audit-trail"].findings == []
    assert reports["audit-trail"].severity == OK


def test_worst_severity_rolls_up(prod, hints, now):
    reports = evaluate(prod, rules(), hints, now)
    assert worst_severity(reports) == CRITICAL
    assert len(reports) == 6


def test_evaluate_handles_an_empty_region(now):
    payload = {"regions": [{"region": "eu-west-1", "streams": []}]}
    assert evaluate(payload, rules(), {}, now) == []
    assert worst_severity([]) == OK


# --- rules merging -----------------------------------------------------------


def test_unknown_rule_key_is_rejected():
    with pytest.raises(RulesError, match="unknown rule"):
        merge_rules({"retention": 24})


def test_rule_type_is_checked():
    with pytest.raises(RulesError, match="must be a number"):
        merge_rules({"stale_consumer_days": "ninety"})


def test_defaults_are_not_mutated():
    merged = merge_rules({"stale_consumer_days": 7})
    assert merged["stale_consumer_days"] == 7
    assert DEFAULT_RULES["stale_consumer_days"] == 90


def test_critical_tag_accepts_key_value_form(prod, now):
    report = evaluate_one(prod, "billing-ledger", now=now, critical_tag="source-of-record=true")
    assert findings(report, "retention.source_of_record")
    missed = evaluate_one(prod, "billing-ledger", now=now, critical_tag="source-of-record=false")
    assert not findings(missed, "retention.source_of_record")


def test_unparseable_consumer_timestamp_is_skipped(prod):
    stream = stream_named(prod, "billing-ledger")
    stream["consumers"][0]["creation_timestamp"] = "not-a-date"
    report = evaluate_stream(
        stream, "eu-central-1", rules(), {}, dt.datetime(2026, 11, 21, tzinfo=dt.UTC)
    )
    assert not findings(report, "consumers.stale_efo")

"""Inventory collection.

Collectors return plain dicts. Nothing downstream of this module knows that
boto3 exists, which is what makes every rule testable from a JSON fixture.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

SCHEMA_VERSION = 1


class CollectionError(Exception):
    """Raised when AWS or a saved payload cannot be read."""


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC).isoformat()
    return str(value)


def _shard_level_metrics(summary: dict) -> list[str]:
    metrics: list[str] = []
    for entry in summary.get("EnhancedMonitoring") or []:
        metrics.extend(entry.get("ShardLevelMetrics") or [])
    return metrics


def _shard(raw: dict) -> dict:
    seq = raw.get("SequenceNumberRange") or {}
    hkr = raw.get("HashKeyRange") or {}
    return {
        "shard_id": raw.get("ShardId"),
        "parent_shard_id": raw.get("ParentShardId"),
        "adjacent_parent_shard_id": raw.get("AdjacentParentShardId"),
        "starting_hash_key": hkr.get("StartingHashKey"),
        "ending_hash_key": hkr.get("EndingHashKey"),
        # A shard with an ending sequence number is closed: it is the remains
        # of a split or merge and carries no new records and no shard-hour.
        "closed": bool(seq.get("EndingSequenceNumber")),
    }


def _consumer(raw: dict) -> dict:
    return {
        "name": raw.get("ConsumerName"),
        "arn": raw.get("ConsumerARN"),
        "status": raw.get("ConsumerStatus"),
        "creation_timestamp": _iso(raw.get("ConsumerCreationTimestamp")),
    }


def collect_region(client, region: str) -> dict:
    """Read every stream in one region through the four describe/list calls."""
    streams: list[dict] = []
    for name in _list_stream_names(client):
        streams.append(_collect_stream(client, name))
    return {"region": region, "streams": streams}


def _list_stream_names(client) -> list[str]:
    names: list[str] = []
    paginator = getattr(client, "get_paginator", None)
    if paginator is not None:
        try:
            for page in client.get_paginator("list_streams").paginate():
                names.extend(page.get("StreamNames") or [])
            return names
        except Exception as exc:  # pragma: no cover - needs a live client
            raise CollectionError(f"list_streams failed: {exc}") from exc
    page = client.list_streams()
    return list(page.get("StreamNames") or [])


def _collect_stream(client, name: str) -> dict:
    summary = client.describe_stream_summary(StreamName=name).get(
        "StreamDescriptionSummary", {}
    )
    encryption = summary.get("EncryptionType") or "NONE"
    stream = {
        "name": name,
        "arn": summary.get("StreamARN"),
        "status": summary.get("StreamStatus"),
        "mode": ((summary.get("StreamModeDetails") or {}).get("StreamMode")) or "PROVISIONED",
        "retention_hours": summary.get("RetentionPeriodHours"),
        "open_shard_count": summary.get("OpenShardCount"),
        "encryption_type": encryption,
        "key_id": summary.get("KeyId"),
        "shard_level_metrics": _shard_level_metrics(summary),
        "shards": [],
        "consumers": [],
        "tags": {},
    }

    shards: list[dict] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"StreamName": name}
        if token:
            kwargs = {"NextToken": token}
        page = client.list_shards(**kwargs)
        shards.extend(_shard(s) for s in page.get("Shards") or [])
        token = page.get("NextToken")
        if not token:
            break
    stream["shards"] = shards

    if stream["arn"]:
        consumers: list[dict] = []
        token = None
        while True:
            kwargs = {"StreamARN": stream["arn"]}
            if token:
                kwargs["NextToken"] = token
            page = client.list_stream_consumers(**kwargs)
            consumers.extend(_consumer(c) for c in page.get("Consumers") or [])
            token = page.get("NextToken")
            if not token:
                break
        stream["consumers"] = consumers

    tags: dict[str, str] = {}
    start: str | None = None
    while True:
        kwargs = {"StreamName": name}
        if start:
            kwargs["ExclusiveStartTagKey"] = start
        page = client.list_tags_for_stream(**kwargs)
        items = page.get("Tags") or []
        for tag in items:
            tags[tag.get("Key")] = tag.get("Value")
        if not page.get("HasMoreTags") or not items:
            break
        start = items[-1].get("Key")
    stream["tags"] = tags
    return stream


def collect(regions: list[str], profile: str | None = None) -> dict:
    """Collect every requested region from AWS."""
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - import guard
        raise CollectionError(
            "boto3 is required for a live run; use --from-json to replay a saved payload"
        ) from exc

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "regions": [],
    }
    for region in regions:
        try:
            client = session.client("kinesis", region_name=region)
            payload["regions"].append(collect_region(client, region))
        except CollectionError:
            raise
        except Exception as exc:
            raise CollectionError(f"{region}: {exc}") from exc
    return payload


def load_payload(path: str) -> dict:
    """Read a payload written by --dump-json, validating enough to fail loudly."""
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise CollectionError(f"{path}: no such file") from exc
    except json.JSONDecodeError as exc:
        raise CollectionError(f"{path}: invalid JSON ({exc.msg} at line {exc.lineno})") from exc
    except OSError as exc:
        raise CollectionError(f"{path}: {exc}") from exc
    return normalize_payload(data, source=path)


def normalize_payload(data: Any, source: str = "payload") -> dict:
    if not isinstance(data, dict):
        raise CollectionError(f"{source}: expected a JSON object at the top level")
    regions = data.get("regions")
    if not isinstance(regions, list):
        raise CollectionError(f"{source}: missing a 'regions' list")
    for region in regions:
        if not isinstance(region, dict):
            raise CollectionError(f"{source}: every region must be an object")
        if not isinstance(region.get("streams"), list):
            raise CollectionError(
                f"{source}: region {region.get('region')!r} has no 'streams' list"
            )
    data.setdefault("schema_version", SCHEMA_VERSION)
    return data

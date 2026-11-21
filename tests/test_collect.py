import datetime as dt

import pytest

from aws_kinesis_stream_audit.collect import (
    CollectionError,
    collect_region,
    load_payload,
    normalize_payload,
)


class FakeKinesis:
    """Enough of the kinesis client to exercise the collector's paging."""

    def __init__(self):
        self.calls = []

    def list_streams(self):
        self.calls.append("list_streams")
        return {"StreamNames": ["orders"], "HasMoreStreams": False}

    def describe_stream_summary(self, StreamName):
        self.calls.append(f"describe:{StreamName}")
        return {
            "StreamDescriptionSummary": {
                "StreamName": StreamName,
                "StreamARN": f"arn:aws:kinesis:eu-west-1:1:stream/{StreamName}",
                "StreamStatus": "ACTIVE",
                "StreamModeDetails": {"StreamMode": "PROVISIONED"},
                "RetentionPeriodHours": 24,
                "OpenShardCount": 2,
                "EncryptionType": "KMS",
                "KeyId": "alias/aws/kinesis",
                "EnhancedMonitoring": [{"ShardLevelMetrics": ["IncomingBytes"]}],
            }
        }

    def list_shards(self, **kwargs):
        self.calls.append(f"list_shards:{sorted(kwargs)}")
        if "NextToken" not in kwargs:
            return {
                "Shards": [
                    {
                        "ShardId": "shardId-000000000000",
                        "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "10"},
                        "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                    }
                ],
                "NextToken": "page2",
            }
        return {
            "Shards": [
                {
                    "ShardId": "shardId-000000000001",
                    "ParentShardId": "shardId-000000000000",
                    "HashKeyRange": {"StartingHashKey": "11", "EndingHashKey": "20"},
                    "SequenceNumberRange": {
                        "StartingSequenceNumber": "1",
                        "EndingSequenceNumber": "9",
                    },
                }
            ]
        }

    def list_stream_consumers(self, **kwargs):
        self.calls.append("list_stream_consumers")
        return {
            "Consumers": [
                {
                    "ConsumerName": "projector",
                    "ConsumerARN": "arn:...:consumer/projector:1",
                    "ConsumerStatus": "ACTIVE",
                    "ConsumerCreationTimestamp": dt.datetime(2026, 1, 1),
                }
            ]
        }

    def list_tags_for_stream(self, **kwargs):
        self.calls.append("list_tags_for_stream")
        return {"Tags": [{"Key": "owner", "Value": "payments"}], "HasMoreTags": False}


def test_collect_region_builds_a_flat_dict():
    payload = collect_region(FakeKinesis(), "eu-west-1")
    assert payload["region"] == "eu-west-1"
    stream = payload["streams"][0]
    assert stream["name"] == "orders"
    assert stream["mode"] == "PROVISIONED"
    assert stream["retention_hours"] == 24
    assert stream["key_id"] == "alias/aws/kinesis"
    assert stream["shard_level_metrics"] == ["IncomingBytes"]
    assert stream["tags"] == {"owner": "payments"}


def test_collect_region_follows_shard_pagination():
    payload = collect_region(FakeKinesis(), "eu-west-1")
    shards = payload["streams"][0]["shards"]
    assert [s["shard_id"] for s in shards] == [
        "shardId-000000000000",
        "shardId-000000000001",
    ]
    assert [s["closed"] for s in shards] == [False, True]


def test_consumer_timestamps_become_iso_utc():
    payload = collect_region(FakeKinesis(), "eu-west-1")
    created = payload["streams"][0]["consumers"][0]["creation_timestamp"]
    assert created.startswith("2026-01-01T00:00:00")
    assert created.endswith("+00:00")


def test_normalize_rejects_a_non_object():
    with pytest.raises(CollectionError, match="JSON object"):
        normalize_payload([])


def test_normalize_requires_regions():
    with pytest.raises(CollectionError, match="regions"):
        normalize_payload({})


def test_normalize_requires_streams_per_region():
    with pytest.raises(CollectionError, match="streams"):
        normalize_payload({"regions": [{"region": "eu-west-1"}]})


def test_normalize_sets_a_schema_version():
    payload = normalize_payload({"regions": []})
    assert payload["schema_version"] == 1


def test_load_payload_round_trips(fixtures_dir):
    payload = load_payload(str(fixtures_dir / "prod.json"))
    assert payload["regions"][0]["region"] == "eu-central-1"
    assert len(payload["regions"][0]["streams"]) == 6


def test_load_payload_reports_a_missing_file():
    with pytest.raises(CollectionError, match="no such file"):
        load_payload("/nonexistent/payload.json")

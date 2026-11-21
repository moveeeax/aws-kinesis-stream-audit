# aws-kinesis-stream-audit

[![ci](https://github.com/moveeeax/aws-kinesis-stream-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/moveeeax/aws-kinesis-stream-audit/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Kinesis Data Stream is the easiest thing in AWS to leave half-configured, because a
half-configured stream still works. Retention sits at the 24-hour default, so the first
time a consumer is down for a weekend the data is simply gone. Encryption is off, or on
the AWS-managed key, which is not the customer-managed CMK the compliance document
claims. Shard count was sized for the pilot and never moved, so producers eat
`ProvisionedThroughputExceeded` while the dashboard stays green. And somebody enabled
enhanced fan-out two migrations ago for a consumer that no longer exists, which is a flat
charge per consumer-shard-hour for nothing.

Every one of those is a static fact, readable from `describe-stream-summary`,
`list-shards`, `list-stream-consumers` and `list-tags-for-stream`. Nothing in AWS reads
them together and tells you. This does.

## What it checks

| Rule | Severity | Fires when |
| --- | --- | --- |
| `retention.default_24h` | warn | Retention is the 24-hour default |
| `retention.below_minimum` | warn | Retention is under `--min-retention-hours` |
| `retention.source_of_record` | critical | A stream tagged `--critical-tag` has under 168h |
| `encryption.disabled` | critical | No server-side encryption at all |
| `encryption.aws_owned_key` | warn | Encrypted with `alias/aws/kinesis`, not your CMK |
| `capacity.under_provisioned` | critical | Open shards cannot carry the declared MB/s |
| `capacity.under_provisioned_records` | critical | Open shards cannot carry the declared records/s |
| `capacity.over_provisioned` | warn | Capacity exceeds the hint by `--overprovision-factor` |
| `capacity.on_demand_oversized` | warn | On-demand, but the declared need fits one shard |
| `consumers.none_registered` | warn | No registered consumer, possibly abandoned |
| `consumers.none_active` | warn | Every registration is in a non-`ACTIVE` state |
| `consumers.not_active` | warn | A named consumer is not `ACTIVE` |
| `consumers.stale_efo` | warn | A fan-out consumer older than `--stale-consumer-days` |
| `consumers.over_fanout_limit` | critical | More registrations than the per-stream limit |
| `observability.shard_metrics_off` | warn | `IncomingBytes` / `WriteProvisionedThroughputExceeded` are not on at shard level |
| `observability.uneven_hash_ranges` | warn | Open shards have lopsided hash-key ranges after a manual reshard |
| `hygiene.not_active` | warn | Stream status is not `ACTIVE` |
| `hygiene.untagged` | warn | No tags, with `--require-tags` |

## How it works

Collection and judgement are separate. `collect.py` turns the four read-only API calls
into a plain dict; `rules.py` reads only that dict. So the entire rule set is tested
against JSON fixtures with no credentials, no `moto` and no network, and a real run can
be captured with `--dump-json` and replayed anywhere with `--from-json`.

Capacity arithmetic lives in `pricing.py` and nowhere else: 1 MB/s and 1,000 records/s
ingress, 2 MB/s egress per open shard, so every shortfall printed in a finding is a
number a unit test asserts. Closed shards — the leftovers of a split or merge — are
excluded from the count, because they carry no throughput and no shard-hour.

The dollar figures are us-east-1 list prices ($0.015 per shard-hour, $0.015 per
enhanced-fan-out consumer-shard-hour, $0.020 per shard-hour for extended retention, from
[the Kinesis pricing page](https://aws.amazon.com/kinesis/data-streams/pricing/)). They
differ per region, so override them in `--rules`.

## Install

```bash
pip install "git+https://github.com/moveeeax/aws-kinesis-stream-audit#egg=aws-kinesis-stream-audit[aws,yaml]"
```

`boto3` is only needed for live runs; `--from-json` works on the standard library alone.

## Usage

```bash
# one region, live
aws-kinesis-stream-audit --region eu-central-1

# several regions, fail the pipeline on anything critical
aws-kinesis-stream-audit --regions eu-central-1,eu-west-1 --fail-on critical

# demand 7 days of retention everywhere, machine-readable
aws-kinesis-stream-audit --region eu-central-1 --min-retention-hours 168 --format json \
  | jq '.streams[] | select(.severity=="critical")'

# capture once, replay offline
aws-kinesis-stream-audit --region eu-central-1 --dump-json prod.json --fail-on none
aws-kinesis-stream-audit --from-json prod.json --format markdown
```

Against the bundled sample payload:

```console
$ aws-kinesis-stream-audit --from-json examples/prod.json \
    --throughput-hints examples/throughput-hints.json
[CRIT] eu-central-1/billing-ledger (PROVISIONED, 1 open shard(s), retention 48h)
    CRIT  retention.source_of_record: tagged 'source-of-record' but retention is 48h, under the 168h this stream's own tag claims it needs
    WARN  encryption.aws_owned_key: encrypted with the AWS-managed key 'alias/aws/kinesis', not a customer-managed CMK: no key policy of yours applies and rotation is not yours to prove
    WARN  consumers.stale_efo: enhanced fan-out consumer 'ledger-archiver' registered 1055 days ago and never re-registered: $10.95/month at 1 shard(s) if nothing reads it

[CRIT] eu-central-1/clickstream-raw (PROVISIONED, 8 open shard(s), retention 168h)
    CRIT  encryption.disabled: server-side encryption is off: records sit unencrypted at rest for the whole retention window
    WARN  capacity.over_provisioned: 8 open shard(s) for a declared 1.0 MB/s that needs 1: 7 excess shard(s) at about $76.65/month
    WARN  consumers.none_registered: no registered consumer at all: either everything reads with GetRecords or the stream is abandoned and still billing shard-hours
    WARN  observability.shard_metrics_off: shard-level metrics missing IncomingBytes, WriteProvisionedThroughputExceeded: a hot shard from a bad partition key cannot be found without them

[CRIT] eu-central-1/orders-events (PROVISIONED, 2 open shard(s), retention 24h)
    WARN  retention.default_24h: retention is the 24h default: a consumer outage longer than a day is unrecoverable, there is nothing to replay. 7 days costs about $29.20/month at 2 shard(s)
    CRIT  capacity.under_provisioned: 2 open shard(s) carry 2.0 MB/s but the stream declares 5.0 MB/s: a 3.0 MB/s shortfall, so producers eat ProvisionedThroughputExceeded and the SDK retries hide it

6 stream(s): 3 critical, 2 warn, 1 clean
```

Clean streams are hidden unless you pass `--show-clean`.

## Declaring throughput

Capacity findings need to know what the stream is *supposed* to carry. Two ways to say it:

- a tag on the stream, `kinesis-throughput-mbps=5`;
- a hints file, `--throughput-hints hints.json` (or YAML):

```json
{
  "orders-events": { "mb_per_s": 5, "records_per_s": 4000 },
  "clickstream-raw": 1.0
}
```

Without a hint, the tool stays quiet about capacity rather than guessing.

## Tuning the thresholds

`examples/rules.yaml` lists every threshold with its default. Pass a subset:

```yaml
min_retention_hours: 168
stale_consumer_days: 30
shard_hour_usd: 0.0168
```

An unknown key or a wrong type is an error, not a silent no-op.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Nothing at or above `--fail-on` |
| 1 | At least one finding at or above `--fail-on` (default `critical`) |
| 2 | Usage error, or an unreadable payload / rules / hints file |

A malformed file prints one line to stderr. There is no path that ends in a traceback.

## IAM

Read-only, four actions:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kinesis:ListStreams",
      "kinesis:DescribeStreamSummary",
      "kinesis:ListShards",
      "kinesis:ListStreamConsumers",
      "kinesis:ListTagsForStream"
    ],
    "Resource": "*"
  }]
}
```

## Development

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check .
pytest -q
```

## License

MIT.

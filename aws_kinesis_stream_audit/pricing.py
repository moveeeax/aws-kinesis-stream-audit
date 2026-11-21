"""Shard capacity and shard-hour cost arithmetic.

Every number a finding prints comes from here so it can be unit-tested
directly. Prices are us-east-1 list prices from
https://aws.amazon.com/kinesis/data-streams/pricing/ and are overridable
through the ``--rules`` file, because they differ per region and change.
"""

from __future__ import annotations

from dataclasses import dataclass

# Per open shard, per the Kinesis Data Streams service limits.
SHARD_INGRESS_MB_S = 1.0
SHARD_INGRESS_RECORDS_S = 1000
SHARD_EGRESS_MB_S = 2.0

# us-east-1 list prices, USD.
SHARD_HOUR_USD = 0.015
EFO_CONSUMER_SHARD_HOUR_USD = 0.015
EXTENDED_RETENTION_SHARD_HOUR_USD = 0.020

HOURS_PER_MONTH = 730


@dataclass(frozen=True)
class Capacity:
    """The throughput ceiling implied by a shard count."""

    shards: int
    ingress_mb_s: float
    ingress_records_s: int
    egress_mb_s: float


def capacity_for(open_shards: int) -> Capacity:
    """Return the hard ceiling a provisioned stream cannot exceed."""
    if open_shards < 0:
        raise ValueError("open_shards must not be negative")
    return Capacity(
        shards=open_shards,
        ingress_mb_s=round(open_shards * SHARD_INGRESS_MB_S, 6),
        ingress_records_s=open_shards * SHARD_INGRESS_RECORDS_S,
        egress_mb_s=round(open_shards * SHARD_EGRESS_MB_S, 6),
    )


def shards_needed(mb_per_s: float, records_per_s: int = 0) -> int:
    """Smallest shard count that carries both halves of a throughput hint."""
    import math

    by_bytes = math.ceil(mb_per_s / SHARD_INGRESS_MB_S) if mb_per_s > 0 else 0
    by_records = (
        math.ceil(records_per_s / SHARD_INGRESS_RECORDS_S) if records_per_s > 0 else 0
    )
    return max(1, by_bytes, by_records)


def shard_hours_monthly(shards: int, hours: int = HOURS_PER_MONTH) -> int:
    return shards * hours


def shard_cost_monthly(shards: int, price: float = SHARD_HOUR_USD) -> float:
    """Monthly shard-hour bill for ``shards`` shards."""
    return round(shard_hours_monthly(shards) * price, 2)


def efo_cost_monthly(
    shards: int, consumers: int = 1, price: float = EFO_CONSUMER_SHARD_HOUR_USD
) -> float:
    """Monthly enhanced fan-out bill: charged per consumer, per shard, per hour."""
    return round(shard_hours_monthly(shards) * consumers * price, 2)


def extended_retention_cost_monthly(
    shards: int, price: float = EXTENDED_RETENTION_SHARD_HOUR_USD
) -> float:
    """Monthly surcharge for retention above the free 24 hours."""
    return round(shard_hours_monthly(shards) * price, 2)


def usd(amount: float) -> str:
    return f"${amount:,.2f}"

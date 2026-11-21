import pytest

from aws_kinesis_stream_audit import pricing


def test_capacity_scales_with_open_shards():
    cap = pricing.capacity_for(4)
    assert cap.ingress_mb_s == 4.0
    assert cap.ingress_records_s == 4000
    assert cap.egress_mb_s == 8.0


def test_zero_shards_has_zero_capacity():
    cap = pricing.capacity_for(0)
    assert (cap.ingress_mb_s, cap.ingress_records_s) == (0.0, 0)


def test_negative_shard_count_is_rejected():
    with pytest.raises(ValueError):
        pricing.capacity_for(-1)


@pytest.mark.parametrize(
    "mb,records,expected",
    [
        (0.4, 0, 1),
        (1.0, 0, 1),
        (1.1, 0, 2),
        (5.0, 0, 5),
        (1.0, 4000, 4),
        (3.0, 1000, 3),
        (0, 0, 1),
    ],
)
def test_shards_needed(mb, records, expected):
    assert pricing.shards_needed(mb, records) == expected


def test_monthly_shard_hours():
    assert pricing.shard_hours_monthly(2) == 1460


def test_shard_cost_uses_the_list_price():
    # 7 shards * 730 h * $0.015
    assert pricing.shard_cost_monthly(7) == 76.65


def test_efo_cost_is_per_consumer_per_shard():
    single = pricing.efo_cost_monthly(2, 1)
    double = pricing.efo_cost_monthly(2, 2)
    assert double == pytest.approx(single * 2)
    assert single == 21.9


def test_extended_retention_cost():
    assert pricing.extended_retention_cost_monthly(1) == 14.6


def test_usd_formats_with_separators():
    assert pricing.usd(1234.5) == "$1,234.50"

import datetime as dt
import json
import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
NOW = dt.datetime(2026, 11, 21, 9, 0, tzinfo=dt.UTC)


def load(name: str) -> dict:
    with (FIXTURES / name).open(encoding="utf-8") as handle:
        return json.load(handle)


@pytest.fixture
def now() -> dt.datetime:
    return NOW


@pytest.fixture
def prod() -> dict:
    return load("prod.json")


@pytest.fixture
def hints() -> dict:
    return load("hints.json")


@pytest.fixture
def fixtures_dir() -> pathlib.Path:
    return FIXTURES


def stream_named(payload: dict, name: str) -> dict:
    for region in payload["regions"]:
        for stream in region["streams"]:
            if stream["name"] == name:
                return json.loads(json.dumps(stream))
    raise KeyError(name)

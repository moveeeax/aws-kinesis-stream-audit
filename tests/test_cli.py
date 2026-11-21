import json

from aws_kinesis_stream_audit.cli import EXIT_FINDINGS, EXIT_OK, EXIT_USAGE, main


def run(capsys, *argv):
    code = main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def test_from_json_reproduces_a_full_run_offline(capsys, fixtures_dir):
    code, out, _ = run(capsys, "--from-json", str(fixtures_dir / "prod.json"))
    assert code == EXIT_FINDINGS
    assert "billing-ledger" in out
    assert "clickstream-raw" in out
    assert "6 stream(s):" in out


def test_fail_on_critical_exits_one(capsys, fixtures_dir):
    code, _, _ = run(
        capsys, "--from-json", str(fixtures_dir / "prod.json"), "--fail-on", "critical"
    )
    assert code == EXIT_FINDINGS


def test_fail_on_critical_exits_zero_without_criticals(capsys, fixtures_dir, tmp_path):
    payload = json.loads((fixtures_dir / "prod.json").read_text())
    streams = payload["regions"][0]["streams"]
    payload["regions"][0]["streams"] = [s for s in streams if s["name"] == "telemetry-ondemand"]
    path = tmp_path / "warn-only.json"
    path.write_text(json.dumps(payload))
    code, out, _ = run(capsys, "--from-json", str(path), "--fail-on", "critical")
    assert code == EXIT_OK
    assert "WARN" in out


def test_fail_on_warn_catches_warnings(capsys, fixtures_dir, tmp_path):
    payload = json.loads((fixtures_dir / "prod.json").read_text())
    streams = payload["regions"][0]["streams"]
    payload["regions"][0]["streams"] = [s for s in streams if s["name"] == "telemetry-ondemand"]
    path = tmp_path / "warn-only.json"
    path.write_text(json.dumps(payload))
    assert run(capsys, "--from-json", str(path), "--fail-on", "warn")[0] == EXIT_FINDINGS


def test_fail_on_none_always_exits_zero(capsys, fixtures_dir):
    code, _, _ = run(capsys, "--from-json", str(fixtures_dir / "prod.json"), "--fail-on", "none")
    assert code == EXIT_OK


def test_clean_payload_exits_zero(capsys, fixtures_dir):
    code, out, _ = run(capsys, "--from-json", str(fixtures_dir / "empty.json"))
    assert code == EXIT_OK
    assert "0 stream(s)" in out


def test_json_output_is_valid_json(capsys, fixtures_dir):
    code, out, _ = run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--throughput-hints",
        str(fixtures_dir / "hints.json"),
        "--format",
        "json",
    )
    assert code == EXIT_FINDINGS
    body = json.loads(out)
    assert body["summary"]["streams"] == 6
    assert body["summary"]["critical"] >= 2
    names = {s["stream"] for s in body["streams"]}
    assert "orders-events" in names
    critical = [s for s in body["streams"] if s["severity"] == "critical"]
    assert all(s["findings"] for s in critical)


def test_markdown_output_has_a_table(capsys, fixtures_dir):
    _, out, _ = run(
        capsys, "--from-json", str(fixtures_dir / "prod.json"), "--format", "markdown"
    )
    assert out.startswith("# Kinesis Data Streams audit")
    assert "| Region | Stream |" in out


def test_clean_streams_are_hidden_by_default(capsys, fixtures_dir):
    _, quiet, _ = run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--throughput-hints",
        str(fixtures_dir / "hints.json"),
    )
    assert "audit-trail" not in quiet
    _, verbose, _ = run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--throughput-hints",
        str(fixtures_dir / "hints.json"),
        "--show-clean",
    )
    assert "audit-trail" in verbose


def test_missing_source_is_a_usage_error(capsys):
    code, _, err = run(capsys)
    assert code == EXIT_USAGE
    assert "--region" in err


def test_from_json_with_regions_is_a_usage_error(capsys, fixtures_dir):
    code, _, err = run(
        capsys, "--from-json", str(fixtures_dir / "prod.json"), "--region", "eu-west-1"
    )
    assert code == EXIT_USAGE
    assert "drop --region" in err


def test_missing_payload_file_is_a_usage_error_not_a_traceback(capsys):
    code, _, err = run(capsys, "--from-json", "/nonexistent/payload.json")
    assert code == EXIT_USAGE
    assert "no such file" in err


def test_malformed_payload_is_reported(capsys, fixtures_dir):
    code, _, err = run(capsys, "--from-json", str(fixtures_dir / "malformed.json"))
    assert code == EXIT_USAGE
    assert "streams" in err


def test_invalid_json_payload_is_reported(capsys, tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not json")
    code, _, err = run(capsys, "--from-json", str(path))
    assert code == EXIT_USAGE
    assert "invalid JSON" in err


def test_bad_rules_file_is_reported(capsys, fixtures_dir, tmp_path):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"stale_consumer_days": "soon"}))
    code, _, err = run(
        capsys, "--from-json", str(fixtures_dir / "prod.json"), "--rules", str(path)
    )
    assert code == EXIT_USAGE
    assert "must be a number" in err


def test_bad_hints_file_is_reported(capsys, fixtures_dir, tmp_path):
    path = tmp_path / "hints.json"
    path.write_text(json.dumps({"orders-events": "lots"}))
    code, _, err = run(
        capsys, "--from-json", str(fixtures_dir / "prod.json"), "--throughput-hints", str(path)
    )
    assert code == EXIT_USAGE
    assert "must be a number or a mapping" in err


def test_rules_file_overrides_a_threshold(capsys, fixtures_dir, tmp_path):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"min_retention_hours": 720, "critical_tag": "none"}))
    code, out, _ = run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--rules",
        str(path),
        "--fail-on",
        "none",
    )
    assert code == EXIT_OK
    assert "under the required 720h" in out


def test_dump_json_writes_the_payload_back(capsys, fixtures_dir, tmp_path):
    out_path = tmp_path / "dump.json"
    run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--dump-json",
        str(out_path),
        "--fail-on",
        "none",
    )
    dumped = json.loads(out_path.read_text())
    assert len(dumped["regions"][0]["streams"]) == 6


def test_scalar_hint_is_accepted(capsys, fixtures_dir, tmp_path):
    path = tmp_path / "hints.json"
    path.write_text(json.dumps({"audit-trail": 9}))
    _, out, _ = run(
        capsys,
        "--from-json",
        str(fixtures_dir / "prod.json"),
        "--throughput-hints",
        str(path),
        "--fail-on",
        "none",
    )
    assert "audit-trail" in out
    assert "MB/s shortfall" in out

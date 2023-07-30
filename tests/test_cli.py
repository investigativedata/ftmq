from pathlib import Path

import orjson
from click.testing import CliRunner
from nomenklatura.entity import CompositeEntity

from ftmq.cli import cli
from ftmq.io import load_proxy

runner = CliRunner()


def _get_lines(output: str) -> list[str]:
    lines = output.strip().split("\n")
    return [li.strip() for li in lines if li.strip()]


def test_cli(fixtures_path: Path):
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0

    in_uri = str(fixtures_path / "eu_authorities.ftm.json")
    result = runner.invoke(cli, ["-i", in_uri, "-d", "eu_authorities"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = load_proxy(orjson.loads(lines[0]))
    assert isinstance(proxy, CompositeEntity)

    result = runner.invoke(cli, ["-i", in_uri, "-d", "other_dataset"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 0

    result = runner.invoke(cli, ["-i", in_uri, "-s", "PublicBody"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151

    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "PublicBody", "--jurisdiction", "eu"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151

    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "PublicBody", "--jurisdiction", "fr"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 0

    in_uri = str(fixtures_path / "ec_meetings.ftm.json")
    result = runner.invoke(cli, ["-i", in_uri, "-s", "Event", "--date__gte", "2022"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 3575


def test_cli_apply(fixtures_path: Path):
    in_uri = str(fixtures_path / "eu_authorities.ftm.json")

    result = runner.invoke(cli, ["apply", "-i", in_uri, "-d", "another_dataset"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = load_proxy(orjson.loads(lines[0]))
    assert isinstance(proxy, CompositeEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" in proxy.datasets

    # replace dataset
    result = runner.invoke(
        cli, ["apply", "-i", in_uri, "-d", "another_dataset", "--replace-dataset"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = load_proxy(orjson.loads(lines[0]))
    assert isinstance(proxy, CompositeEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" not in proxy.datasets


def test_cli_coverage(fixtures_path: Path):
    in_uri = str(fixtures_path / "ec_meetings.ftm.json")
    result = runner.invoke(
        cli, ["-i", in_uri, "-o", "/dev/null", "--coverage-uri", "-"]
    )
    assert orjson.loads(result.output) == {
        "start": "2014-11-12",
        "end": "2023-01-20",
        "countries": ["eu"],
        "frequency": "unknown",
        "schemata": {
            "Address": 1281,
            "PublicBody": 103,
            "Event": 34975,
            "Membership": 791,
            "Person": 791,
            "Organization": 7097,
        },
        "entities": 45038,
    }

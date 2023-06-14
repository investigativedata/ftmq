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

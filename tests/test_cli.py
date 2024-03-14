from pathlib import Path

import orjson
from click.testing import CliRunner
from nomenklatura.entity import CompositeEntity

from ftmq.cli import cli
from ftmq.io import make_proxy

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
    proxy = make_proxy(orjson.loads(lines[0]))
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

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-s", "Payment", "--date__gte", "2010"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 49

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-s", "Person", "--sort", "name"])
    lines = _get_lines(result.output)
    data = orjson.loads(lines[0])
    assert data["caption"] == "Dr.-Ing. E. h. Martin Herrenknecht"
    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "Person", "--sort", "name", "--sort-descending"]
    )
    lines = _get_lines(result.output)
    data = orjson.loads(lines[0])
    assert data["caption"] == "Johanna Quandt"


def test_cli_apply(fixtures_path: Path):
    in_uri = str(fixtures_path / "eu_authorities.ftm.json")

    result = runner.invoke(cli, ["apply", "-i", in_uri, "-d", "another_dataset"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = make_proxy(orjson.loads(lines[0]))
    assert isinstance(proxy, CompositeEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" in proxy.datasets
    assert "default" not in proxy.datasets

    # replace dataset
    result = runner.invoke(
        cli, ["apply", "-i", in_uri, "-d", "another_dataset", "--replace-dataset"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = make_proxy(orjson.loads(lines[0]))
    assert isinstance(proxy, CompositeEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" not in proxy.datasets
    assert "default" not in proxy.datasets


def test_cli_coverage(fixtures_path: Path):
    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-o", "/dev/null", "--stats-uri", "-"])
    assert result.exit_code == 0
    test_result = orjson.loads(result.output)
    test_result["coverage"]["countries"] = sorted(test_result["coverage"]["countries"])
    test_result["things"]["countries"] = sorted(
        test_result["things"]["countries"], key=lambda x: x["code"]
    )
    test_result["things"]["schemata"] = sorted(
        test_result["things"]["schemata"], key=lambda x: x["name"]
    )
    assert test_result == {
        "coverage": {
            "start": "2002-07-04",
            "end": "2011-12-29",
            "frequency": "unknown",
            "countries": ["cy", "de", "gb", "lu"],
            "schedule": None,
        },
        "things": {
            "total": 184,
            "countries": [
                {"code": "cy", "count": 2, "label": "Cyprus"},
                {"code": "de", "count": 163, "label": "Germany"},
                {"code": "gb", "count": 3, "label": "United Kingdom"},
                {"code": "lu", "count": 2, "label": "Luxembourg"},
            ],
            "schemata": [
                {
                    "name": "Address",
                    "count": 89,
                    "label": "Address",
                    "plural": "Addresses",
                },
                {
                    "name": "Company",
                    "count": 56,
                    "label": "Company",
                    "plural": "Companies",
                },
                {
                    "name": "Organization",
                    "count": 17,
                    "label": "Organization",
                    "plural": "Organizations",
                },
                {"name": "Person", "count": 22, "label": "Person", "plural": "People"},
            ],
        },
        "intervals": {
            "total": 290,
            "countries": [],
            "schemata": [
                {
                    "name": "Payment",
                    "count": 290,
                    "label": "Payment",
                    "plural": "Payments",
                }
            ],
        },
        "entity_count": 474,
    }


def test_cli_aggregation(fixtures_path: Path):
    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(
        cli,
        [
            "-i",
            in_uri,
            "-o",
            "/dev/null",
            "--aggregation-uri",
            "-",
            "--sum",
            "amountEur",
        ],
    )
    assert result.exit_code == 0
    result = orjson.loads(result.output)
    assert result == {"sum": {"amountEur": 40589689.15}}

    result = runner.invoke(
        cli,
        [
            "-i",
            in_uri,
            "-o",
            "/dev/null",
            "--aggregation-uri",
            "-",
            "--max",
            "name",
            "--groups",
            "country",
        ],
    )
    assert result.exit_code == 0
    result = orjson.loads(result.output)
    assert result == {
        "max": {"name": "YOC AG"},
        "groups": {
            "country": {
                "max": {
                    "name": {
                        "de": "YOC AG",
                        "cy": "Schoeller Holdings Ltd.",
                        "gb": "Matthias Rath Limited",
                        "lu": "Eurolottoclub AG",
                    }
                }
            }
        },
    }

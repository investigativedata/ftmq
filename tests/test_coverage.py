from pathlib import Path

from ftmq.coverage import Coverage
from ftmq.io import smart_read_proxies


def test_coverage(fixtures_path: Path):
    with Coverage() as c:
        for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
            c.collect(proxy)

    assert c.to_dict() == {
        "start": "2014-11-12",
        "end": "2023-01-20",
        "countries": ["eu"],
        "frequency": "unknown",
        "entities": 45038,
        "schemata": {
            "Address": 1281,
            "PublicBody": 103,
            "Event": 34975,
            "Membership": 791,
            "Person": 791,
            "Organization": 7097,
        },
    }

    coverage = Coverage()
    with coverage as c:
        for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
            c.collect(proxy)

    assert coverage.to_dict() == {
        "start": "2014-11-12",
        "end": "2023-01-20",
        "countries": ["eu"],
        "frequency": "unknown",
        "entities": 45038,
        "schemata": {
            "Address": 1281,
            "PublicBody": 103,
            "Event": 34975,
            "Membership": 791,
            "Person": 791,
            "Organization": 7097,
        },
    }

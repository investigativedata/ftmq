from datetime import date
from pathlib import Path

from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector, Coverage


def test_coverage(fixtures_path: Path):
    with Coverage() as c:
        for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
            c.collect(proxy)

    start = date(2014, 11, 12)
    end = date(2023, 1, 20)

    assert c.to_dict() == {
        "start": start,
        "end": end,
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

    assert coverage.dict() == {
        "start": start,
        "end": end,
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

    # nk pass through
    assert coverage.to_dict() == {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "countries": ["eu"],
        "frequency": "unknown",
    }

    proxies = smart_read_proxies(fixtures_path / "ec_meetings.ftm.json")
    collector = Collector()
    proxies = collector.apply(proxies)
    len_proxies = len([x for x in proxies])
    coverage = collector.export()
    assert coverage.entities > 0
    assert coverage.entities == len_proxies

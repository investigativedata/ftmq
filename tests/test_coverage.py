from datetime import date
from pathlib import Path

from nomenklatura.dataset.coverage import DataCoverage

from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector, DatasetStats


def test_coverage(fixtures_path: Path):
    c = Collector()
    for proxy in smart_read_proxies(fixtures_path / "ec_meetings.ftm.json"):
        c.collect(proxy)

    start = date(2014, 11, 12)
    end = date(2023, 1, 20)
    result = {
        "coverage": {
            "start": start,
            "end": end,
            "frequency": "unknown",
            "countries": ["eu"],
            "schedule": None,
        },
        "things": {
            "total": 44247,
            "countries": [{"code": "eu", "count": 103, "label": "eu"}],
            "schemata": [
                {
                    "name": "Address",
                    "count": 1281,
                    "label": "Address",
                    "plural": "Addresses",
                },
                {
                    "name": "PublicBody",
                    "count": 103,
                    "label": "Public body",
                    "plural": "Public bodies",
                },
                {"name": "Event", "count": 34975, "label": "Event", "plural": "Events"},
                {"name": "Person", "count": 791, "label": "Person", "plural": "People"},
                {
                    "name": "Organization",
                    "count": 7097,
                    "label": "Organization",
                    "plural": "Organizations",
                },
            ],
        },
        "intervals": {
            "total": 791,
            "countries": [],
            "schemata": [
                {
                    "name": "Membership",
                    "count": 791,
                    "label": "Membership",
                    "plural": "Memberships",
                }
            ],
        },
        "entity_count": 45038,
    }

    assert isinstance(c.export(), DatasetStats)
    assert c.to_dict() == result

    proxies = smart_read_proxies(fixtures_path / "ec_meetings.ftm.json")
    collector = Collector()
    proxies = collector.apply(proxies)
    len_proxies = len([x for x in proxies])
    stats = collector.export()
    assert stats.entity_count > 0
    assert stats.entity_count == len_proxies
    assert stats.coverage.years == (2014, 2023)

    # align with nomenklatura
    nk_coverage = DataCoverage(stats.coverage.model_dump())
    assert nk_coverage.to_dict() == {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "countries": ["eu"],
        "frequency": "unknown",
    }

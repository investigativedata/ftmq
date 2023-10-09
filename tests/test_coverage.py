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
    result = {
        "start": start,
        "end": end,
        "years": (2014, 2023),
        "frequency": "unknown",
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
            {
                "name": "Membership",
                "count": 791,
                "label": "Membership",
                "plural": "Memberships",
            },
            {"name": "Person", "count": 791, "label": "Person", "plural": "People"},
            {
                "name": "Organization",
                "count": 7097,
                "label": "Organization",
                "plural": "Organizations",
            },
        ],
        "countries": [{"code": "eu", "count": 103, "label": "eu"}],
        "entities": 45038,
    }

    assert isinstance(c.export(), Coverage)
    assert c.to_dict() == result

    proxies = smart_read_proxies(fixtures_path / "ec_meetings.ftm.json")
    collector = Collector()
    proxies = collector.apply(proxies)
    len_proxies = len([x for x in proxies])
    coverage = collector.export()
    assert coverage.entities > 0
    assert coverage.entities == len_proxies

    assert coverage.to_dict() == {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "countries": ["eu"],
        "frequency": "unknown",
    }

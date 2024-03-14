from datetime import date
from pathlib import Path

from nomenklatura.dataset.coverage import DataCoverage

from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector, DatasetStats


def test_coverage(fixtures_path: Path):
    c = Collector()
    for proxy in smart_read_proxies(fixtures_path / "donations.ijson"):
        c.collect(proxy)

    start = date(2002, 7, 4)
    end = date(2011, 12, 29)
    result = {
        "coverage": {
            "start": start,
            "end": end,
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

    assert isinstance(c.export(), DatasetStats)
    test_result = c.to_dict()
    test_result["coverage"]["countries"] = sorted(test_result["coverage"]["countries"])
    test_result["things"]["countries"] = sorted(
        test_result["things"]["countries"], key=lambda x: x["code"]
    )
    test_result["things"]["schemata"] = sorted(
        test_result["things"]["schemata"], key=lambda x: x["name"]
    )
    assert test_result == result

    proxies = smart_read_proxies(fixtures_path / "donations.ijson")
    collector = Collector()
    proxies = collector.apply(proxies)
    len_proxies = len([x for x in proxies])
    stats = collector.export()
    assert stats.entity_count > 0
    assert stats.entity_count == len_proxies
    assert stats.coverage.years == (2002, 2011)

    # align with nomenklatura
    nk_coverage = DataCoverage(stats.coverage.model_dump())
    test_result = nk_coverage.to_dict()
    test_result["countries"] = sorted(test_result["countries"])
    result = {
        "start": "2002-07-04",
        "end": "2011-12-29",
        "countries": ["cy", "de", "gb", "lu"],
        "frequency": "unknown",
    }

from ftmq.aggregations import Aggregation, Aggregator


def test_agg(donations):
    values = {
        "sum": 40589689.15,
        "min": 50000,
        "max": 2334526,
        "avg": 139964.44534482757,
    }

    for key, value in values.items():
        with Aggregation(prop="amountEur", func=key) as agg:
            for proxy in donations:
                agg.collect(proxy)
        assert agg.value == value

    agg = Aggregation(prop="date", func="min")
    assert isinstance(hash(agg), int)
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    assert agg.value == "2002-07-04"

    agg = Aggregator.from_dict({key: ["amountEur"] for key in values})
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    tested = False
    for key, value in values.items():
        assert agg.result[key]["amountEur"] == value
        tested = True
    assert tested

    agg = Aggregator.from_dict({"count": ["country"]})
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    assert agg.result["count"] == {"country": 4}


def test_agg_groupby(donations):
    with Aggregation(prop="name", func="count", group_props=["country"]) as agg:
        assert isinstance(hash(agg), int)
        for proxy in donations:
            agg.collect(proxy)
    assert agg.dict()["groups"] == {"country": {"de": 80, "cy": 1, "gb": 1, "lu": 1}}

    agg = Aggregator.from_dict({"count": ["name"], "groups": ["country"]})
    assert agg.to_dict() == {
        "groups": {"country": {"count": {"name"}}},
        "count": {"name"},
    }
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    assert agg.result == {
        "country": {"name": {"de": 80, "cy": 1, "gb": 1, "lu": 1}},
        "count": {"name": 95},
    }

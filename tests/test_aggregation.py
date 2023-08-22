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
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    assert agg.value == "2002-07-04"

    agg = Aggregator(
        aggregations=[Aggregation(prop="amountEur", func=key) for key in values]
    )
    proxies = agg.apply(donations)
    _ = [x for x in proxies]
    tested = False
    for key, value in values.items():
        assert agg.result[key]["amountEur"] == value
        tested = True
    assert tested

from ftmq.query import Query


def test_search(eu_authorities):
    q = Query().where(dataset="eu_authorities")
    proxies = q.apply_iter(eu_authorities)
    assert len(eu_authorities) == len([p for p in proxies])

    q = Query().where(dataset="eu_authorities")
    s = q.search("agency")
    proxies = s.apply_iter(eu_authorities)
    assert len([p for p in proxies]) == 23

    q = Query().where(dataset="eu_authorities")
    s = q.search("agency", ["name"])
    proxies = s.apply_iter(eu_authorities)
    assert len([p for p in proxies]) == 23

    q = Query().where(dataset="eu_authorities")
    s = q.search("agency", ["location", "date"])
    proxies = s.apply_iter(eu_authorities)
    assert len([p for p in proxies]) == 0

    # filters come before search
    q = Query().where(dataset="gdho")
    s = q.search("agency")
    proxies = s.apply_iter(eu_authorities)
    assert len([p for p in proxies]) == 0

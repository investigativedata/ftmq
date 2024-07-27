from nomenklatura.entity import CompositeEntity

from ftmq.model import Catalog, Dataset
from ftmq.query import Query
from ftmq.store import AlephStore, MemoryStore, SQLStore, Store, get_store
from ftmq.store.level import LevelDBStore
from ftmq.util import make_dataset

# from ftmq.store.redis import RedisStore


def _run_store_test_implicit(cls: Store, proxies, **kwargs):
    # implicit catalog from store content
    store = cls(**kwargs)
    assert not store.get_catalog().names

    datasets_seen = set()
    with store.writer() as bulk:
        for proxy in proxies:
            if proxy.datasets - datasets_seen:
                bulk.add_entity(proxy)
                datasets_seen.update(proxy.datasets)

    assert store.get_catalog().names == {"donations", "eu_authorities"}
    return True


def _run_store_test(cls: Store, proxies, **kwargs):
    # explicit catalog
    catalog = Catalog(
        datasets=[Dataset(name="eu_authorities"), Dataset(name="donations")]
    )
    store = cls(catalog=catalog, **kwargs)
    with store.writer() as bulk:
        for proxy in proxies:
            bulk.add_entity(proxy)
    view = store.default_view()
    properties = view.get_entity("eu-authorities-satcen").to_dict()["properties"]
    assert properties == {
        "legalForm": ["security_agency"],
        "keywords": ["security_agency"],
        "website": ["https://www.satcen.europa.eu/"],
        "description": [
            "The European Union Satellite Centre (SatCen) supports EU decision-making and\naction in the context of Europe’s Common Foreign and Security Policy. This\nmeans providing products and services based on exploiting space assets and\ncollateral data, including satellite imagery and aerial imagery, and related\nservices."  # noqa
        ],
        "name": ["European Union Satellite Centre"],
        "weakAlias": ["SatCen"],
        "jurisdiction": ["eu"],
        "sourceUrl": ["https://www.asktheeu.org/en/body/satcen"],
    }
    assert store.dataset.leaf_names == {"donations", "eu_authorities"}
    tested = False
    for proxy in store.default_view().entities():
        assert isinstance(proxy, CompositeEntity)
        tested = True
        break
    assert tested

    # iterate
    entities = [e for e in store.iterate()]
    assert len(entities) == 474 + 151
    entities = [e for e in store.iterate(dataset="eu_authorities")]
    assert len(entities) == 151

    view = store.default_view()
    ds = make_dataset("eu_authorities")
    view = store.view(ds)
    assert len([e for e in view.entities()]) == 151

    view = store.query()
    q = Query().where(dataset="eu_authorities")
    res = [e for e in view.entities(q)]
    assert len(res) == 151
    assert "eu_authorities" in res[0].datasets
    q = Query().where(schema="Payment", prop="date", value=2011, comparator="gte")
    res = [e for e in view.entities(q)]
    assert all(r.schema.name == "Payment" for r in res)
    assert len(res) == 21

    # stats
    q = Query().where(dataset="eu_authorities")
    stats = view.stats(q)
    assert [c.model_dump() for c in stats.things.countries] == [
        {"code": "eu", "label": "eu", "count": 151}
    ]
    assert stats.entity_count == 151
    assert [s.model_dump() for s in stats.things.schemata] == [
        {
            "name": "PublicBody",
            "label": "Public body",
            "plural": "Public bodies",
            "count": 151,
        }
    ]

    # ordering
    q = Query().where(schema="Payment", prop="date", value=2011, comparator="gte")
    q = q.order_by("amountEur")
    res = [e for e in view.entities(q)]
    assert len(res) == 21
    assert res[0].get("amountEur") == ["50001"]
    q = q.order_by("amountEur", ascending=False)
    res = [e for e in view.entities(q)]
    assert len(res) == 21
    assert res[0].get("amountEur") == ["320000"]

    # slice
    q = Query().where(schema="Payment", prop="date", value=2011, comparator="gte")
    q = q.order_by("amountEur")
    q = q[:10]
    res = [e for e in view.entities(q)]
    assert len(res) == 10
    assert res[0].get("payer") == ["efccc434cdf141c7ba6f6e539bb6b42ecd97c368"]

    q = Query().where(schema="Person").order_by("name")[0]
    res = [e for e in view.entities(q)]
    assert len(res) == 1
    assert res[0].caption == "Dr.-Ing. E. h. Martin Herrenknecht"

    # aggregation
    q = Query().aggregate("max", "date").aggregate("min", "date")
    res = view.aggregations(q)
    assert res == {"max": {"date": "2011-12-29"}, "min": {"date": "2002-07-04"}}

    q = Query().aggregate("count", "id", groups="beneficiary")
    res = view.aggregations(q)
    assert (
        res["groups"]["beneficiary"]["count"]["id"][
            "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8"
        ]
        == 10
    )
    assert len(proxies) == res["count"]["id"]

    q = (
        Query()
        .where(dataset="donations")
        .aggregate("sum", "amountEur", groups="beneficiary")
    )
    res = view.aggregations(q)
    assert res == {
        "groups": {
            "beneficiary": {
                "sum": {
                    "amountEur": {
                        "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8": 3368136.15,
                        "783d918df9f9178400d6b3386439ab3b3679979c": 6039987,
                        "6d8377d3938b85fa1bfd1985486f0f913c42e224": 6394282,
                        "d10764ddf47ca220527d385fc8fbaa62114408e4": 660008,
                        "7202347006660188aab5c1e264c4bee948478fd6": 4125977,
                        "c326dd8021ee75fe9608f31ecb4e2e7388144102": 17231420,
                        "542c6435219bd84c061ea407a6ab1e29b4d146d0": 1030898,
                        "9fbaa5733790781e56eec4998aeacf5093dccbf5": 290725,
                        "9e292c150c617eec85e5479c5f039f8441569441": 175000,
                        "49d46f7e70e19bc497a17734af53ea1a00c831d6": 1221256,
                        "4b308dc2b128377e63a4bf2e4c1b9fcd59614eee": 52000,  # pytest: MAX_SQL_AGG_GROUPS=11
                    }
                }
            }
        },
        "sum": {"amountEur": 40589689.15},
    }
    q = Query().where(dataset="donations").aggregate("sum", "amountEur", groups="year")
    res = view.aggregations(q)
    assert res == {
        "groups": {
            "year": {
                "sum": {
                    "amountEur": {
                        "2011": 1953402.15,
                        "2010": 3899002,
                        "2009": 6451130,
                        "2008": 6002766,
                        "2007": 3266005,
                        "2006": 4515084,
                        "2005": 7278646,
                        "2004": 2156628,
                        "2003": 2337982,
                        "2002": 2729044,
                    }
                }
            }
        },
        "sum": {"amountEur": 40589689.15},
    }

    q = Query().where(dataset="donations").aggregate("avg", "amountEur")
    res = view.aggregations(q)
    assert res == {"avg": {"amountEur": 139964.44534482757}}

    # reversed
    entity_id = "783d918df9f9178400d6b3386439ab3b3679979c"
    q = Query().where(reverse=entity_id)
    res = [p for p in view.entities(q)]
    assert len(res) == 53
    tested = False
    for proxy in res:
        assert entity_id in proxy.get("beneficiary")
        tested = True
    assert tested

    q = Query().where(reverse=entity_id, schema="Payment")
    q = q.where(prop="date", value=2007, comparator="gte")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 37
    q = Query().where(reverse=entity_id, schema="Person")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 0

    # search
    q = Query().where(dataset="eu_authorities").search("agency")
    res = [p for p in view.entities(q)]
    assert len(res) == 23

    # ids
    q = Query().where(entity_id="eu-authorities-chafea")
    res = [p for p in view.entities(q)]
    assert len(res) == 1
    q = Query().where(canonical_id="eu-authorities-chafea")
    res = [p for p in view.entities(q)]
    assert len(res) == 1
    q = Query().where(entity_id="eu-authorities-chafea", dataset="donations")
    res = [p for p in view.entities(q)]
    assert len(res) == 0
    q = Query().where(canonical_id="eu-authorities-chafea", dataset="donations")
    res = [p for p in view.entities(q)]
    assert len(res) == 0
    q = Query().where(entity_id__startswith="eu-authorities-")
    res = [p for p in view.entities(q)]
    assert len(res) == 151
    q = Query().where(canonical_id__startswith="eu-authorities-")
    res = [p for p in view.entities(q)]
    assert len(res) == 151

    return True


def test_store_memory(proxies):
    assert _run_store_test_implicit(MemoryStore, proxies)
    assert _run_store_test(MemoryStore, proxies)


def test_store_leveldb(tmp_path, proxies):
    path = tmp_path / "level.db"
    assert _run_store_test_implicit(MemoryStore, proxies)
    path = tmp_path / "level2.db"
    assert _run_store_test(LevelDBStore, proxies, path=path)


def test_store_sql_sqlite(tmp_path, proxies):
    uri = f"sqlite:///{tmp_path}/test.db"
    assert _run_store_test_implicit(SQLStore, proxies, uri=uri)

    from nomenklatura.db import get_metadata

    get_metadata.cache_clear()
    assert _run_store_test(SQLStore, proxies, uri=uri)


def test_store_init(tmp_path):
    store = get_store()
    assert isinstance(store, MemoryStore)
    store = get_store("memory:///")
    assert isinstance(store, MemoryStore)
    path = tmp_path / "level.db"
    store = get_store(f"leveldb://{path}")
    assert isinstance(store, LevelDBStore)
    store = get_store("sqlite:///:memory:")
    assert isinstance(store, SQLStore)
    store = get_store(dataset="test_dataset")
    assert store.dataset.name == "test_dataset"
    store = get_store("http+aleph://test_dataset@aleph.example.org")
    assert isinstance(store, AlephStore)
    assert store.dataset.name == "test_dataset"

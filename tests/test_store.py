from nomenklatura.entity import CompositeEntity

from ftmq.model import Catalog, Dataset
from ftmq.query import Query
from ftmq.store import AlephStore, LevelDBStore, MemoryStore, SQLStore, Store, get_store


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

    assert store.get_catalog().names == {"ec_meetings", "eu_authorities"}
    return True


def _run_store_test(cls: Store, proxies, **kwargs):
    # explicit catalog
    catalog = Catalog(
        datasets=[Dataset(name="eu_authorities"), Dataset(name="ec_meetings")]
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
    assert store.dataset.leaf_names == {"ec_meetings", "eu_authorities"}
    tested = False
    for proxy in store.iterate():
        assert isinstance(proxy, CompositeEntity)
        tested = True
        break
    assert tested

    view = store.default_view()
    ds = Dataset(name="eu_authorities").to_nk()
    view = store.view(ds)
    assert len([e for e in view.entities()]) == 151

    view = store.query()
    q = Query().where(dataset="eu_authorities")
    res = [e for e in view.entities(q)]
    assert len(res) == 151
    assert "eu_authorities" in res[0].datasets
    q = Query().where(schema="Event", prop="date", value=2023, comparator="gte")
    res = [e for e in view.entities(q)]
    assert res[0].schema.name == "Event"
    assert len(res) == 76

    # coverage
    q = Query().where(dataset="eu_authorities")
    coverage = view.coverage(q)
    assert coverage.countries == [{"code": "eu", "label": "eu", "count": 151}]
    assert coverage.entities == 151
    assert coverage.schemata == [
        {
            "name": "PublicBody",
            "label": "Public body",
            "plural": "Public bodies",
            "count": 151,
        }
    ]

    # ordering
    q = Query().where(schema="Event", prop="date", value=2023, comparator="gte")
    q = q.order_by("location")
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["Abu Dhabi, UAE"]
    q = q.order_by("location", ascending=False)
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["virtual"]

    # slice
    q = Query().where(schema="Event", prop="date", value=2023, comparator="gte")
    q = q.order_by("location")
    q = q[:10]
    res = [e for e in view.entities(q)]
    assert len(res) == 10
    assert res[0].get("location") == ["Abu Dhabi, UAE"]

    # aggregation
    q = Query().aggregate("max", "date").aggregate("min", "date")
    res = view.aggregations(q)
    assert res == {"max": {"date": "2023-01-20"}, "min": {"date": "2014-11-12"}}

    # reversed
    entity_id = "eu-tr-09571422185-81"
    q = Query().where(reverse=entity_id)
    res = [p for p in view.entities(q)]
    assert len(res) == 13
    tested = False
    for proxy in res:
        assert entity_id in proxy.get("involved")
        tested = True
    assert tested

    q = Query().where(reverse=entity_id, schema="Event")
    q = q.where(prop="date", value=2022, comparator="gte")
    res = [p for p in view.entities(q)]
    assert len(res) == 3
    q = Query().where(reverse=entity_id, schema="Person")
    res = [p for p in view.entities(q)]
    assert len(res) == 0

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

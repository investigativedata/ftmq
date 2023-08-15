from nomenklatura.entity import CompositeEntity

from ftmq.model import Catalog, Dataset
from ftmq.query import Query
from ftmq.store import AlephStore, LevelDBStore, MemoryStore, SqlStore, Store, get_store


def _run_store_test(cls: Store, proxies, **kwargs):
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
    q = Query().where(schema="Event", prop="date", value=2023, operator="gte")
    res = [e for e in view.entities(q)]
    assert res[0].schema.name == "Event"
    assert len(res) == 76

    # coverage
    q = Query().where(dataset="eu_authorities")
    coverage = view.coverage(q)
    assert coverage.countries == ["eu"]
    assert coverage.entities == 151
    assert coverage.schemata == {"PublicBody": 151}

    # ordering
    q = Query().where(schema="Event", prop="date", value=2023, operator="gte")
    q = q.order_by("location")
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["Abu Dhabi, UAE"]
    q = q.order_by("location", ascending=False)
    res = [e for e in view.entities(q)]
    assert len(res) == 76
    assert res[0].get("location") == ["virtual"]

    return True


def test_store_memory(proxies):
    assert _run_store_test(MemoryStore, proxies)


def test_store_leveldb(tmp_path, proxies):
    path = tmp_path / "level.db"
    assert _run_store_test(LevelDBStore, proxies, path=path)


def test_store_sql_sqlite(tmp_path, proxies):
    uri = f"sqlite:///{tmp_path}/test.db"
    assert _run_store_test(SqlStore, proxies, uri=uri)


def test_store_init(tmp_path):
    store = get_store()
    assert isinstance(store, MemoryStore)
    store = get_store("memory:///")
    assert isinstance(store, MemoryStore)
    path = tmp_path / "level.db"
    store = get_store(f"leveldb://{path}")
    assert isinstance(store, LevelDBStore)
    store = get_store("sqlite:///:memory:")
    assert isinstance(store, SqlStore)
    store = get_store(dataset="test_dataset")
    assert store.dataset.name == "test_dataset"
    store = get_store("http+aleph://test_dataset@aleph.example.org")
    assert isinstance(store, AlephStore)
    assert store.dataset.name == "test_dataset"

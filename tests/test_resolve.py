from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver

from ftmq.store import get_store
from ftmq.util import make_proxy


def test_resolve():
    resolver = Resolver()
    resolver.decide("a", "b", Judgement.POSITIVE)
    resolver.decide("a", "c", Judgement.NEGATIVE)

    entities = [
        {
            "id": "a",
            "schema": "LegalEntity",
            "properties": {"name": ["Name 1"]},
            "datasets": ["dataset1"],
        },
        {
            "id": "b",
            "schema": "Person",
            "properties": {"name": ["Name 2"]},
            "datasets": ["dataset2"],
        },
        {
            "id": "c",
            "schema": "Person",
            "properties": {"name": ["Name 3"]},
            "datasets": ["dataset2"],
        },
    ]

    store = get_store("memory:///", resolver=resolver)
    with store.writer() as bulk:
        for entity in entities:
            bulk.add_entity(make_proxy(entity))

    store.update(store.resolver.get_canonical("a"))

    catalog = store.get_catalog()
    assert catalog.names == {"dataset1", "dataset2"}
    view = store.view(catalog.get_scope())
    entities = [e for e in view.entities()]
    assert len(entities) == 2

import pytest
from followthemoney import model

from ftmq.exceptions import ValidationError
from ftmq.io import make_proxy
from ftmq.query import Query


def test_proxy_composite():
    data = {"id": "1", "schema": "Thing", "properties": {"name": "Test"}}
    proxy = make_proxy(data)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"default"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test"},
        "datasets": ["test_dataset"],
    }
    proxy = make_proxy(data)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"test_dataset"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test"},
        "datasets": ["test_dataset", "ds2"],
    }
    proxy = make_proxy(data, "another_dataset")
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"another_dataset", "ds2", "test_dataset"}


def test_proxy_filter_dataset(proxies):
    q = Query()
    result = list(filter(q.apply, proxies))
    assert len(result) == 45189

    q = q.where(dataset="eu_authorities")
    result = list(filter(q.apply, proxies))
    assert len(result) == 151


def test_proxy_filter_schema(proxies):
    q = Query().where(schema="Event")
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975

    q = Query().where(schema="Organization")
    result = list(filter(q.apply, proxies))
    assert len(result) == 7097

    q = Query().where(schema__in=["Event", "Organization"])
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975 + 7097

    q = Query().where(schema="Organization", include_matchable=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 7351

    q = Query().where(schema="LegalEntity")
    result = list(filter(q.apply, proxies))
    assert len(result) == 0

    q = Query().where(schema="LegalEntity", include_matchable=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 8142

    q = Query().where(schema="LegalEntity", include_descendants=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 8142

    q = Query().where(schema=model.get("Person"))
    result = list(filter(q.apply, proxies))
    assert len(result) == 791

    q = Query().where(schema__startswith="Pers")
    result = list(filter(q.apply, proxies))
    assert len(result) == 791

    # invalid
    with pytest.raises(ValidationError):
        q = Query().where(schema="Invalid schema")


def test_proxy_filter_property(proxies):
    q = Query().where(prop="jurisdiction", value="eu")
    result = list(filter(q.apply, proxies))
    assert len(result) == 254

    q = Query().where(prop="date", value="2022", comparator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3575

    q = Query().where(prop="date", value="2022", comparator="gt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3575

    # chained same props as AND
    q = q.where(prop="date", value="2023", comparator="lt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3499

    q = Query().where(prop="date", value=2023, comparator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 76

    q = Query().where(prop="date", value=True, comparator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975

    q = Query().where(prop="date", value=False, comparator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975

    q = Query().where(prop="full", value="Brux", comparator="startswith")
    result = list(filter(q.apply, proxies))
    assert len(result) == 12

    q = Query().where(prop="full", value="elles", comparator="endswith")
    result = list(filter(q.apply, proxies))
    assert len(result) == 12

    q = Query().where(prop="full", value="Bruxelles", comparator="not")
    result = list(filter(q.apply, proxies))
    assert len(result) == 1279


def test_proxy_filters_combined(proxies):
    q = Query().where(prop="jurisdiction", value="eu")
    q = q.where(schema="Event")
    result = list(filter(q.apply, proxies))
    assert len(result) == 0


def test_proxy_sort(proxies):
    tested = False
    q = Query().where(schema="Person").order_by("name")
    for proxy in q.apply_iter(proxies):
        assert proxy.caption == "Aare Järvan"
        tested = True
        break
    assert tested
    q = Query().where(schema="Person").order_by("name", ascending=False)
    for proxy in q.apply_iter(proxies):
        assert proxy.caption == "Zaneta Vegnere"
        tested = True
        break
    assert tested


def test_proxy_slice(proxies):
    q = Query()[:10]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 10
    q = Query()[10:20]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 10
    q = Query().where(schema="Person").order_by("name")[0]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 1
    assert res[0].caption == "Aare Järvan"


def test_proxy_filter_reverse(proxies):
    entity_id = "eu-tr-09571422185-81"
    q = Query().where(reverse=entity_id)
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 13
    tested = False
    for proxy in res:
        assert entity_id in proxy.get("involved")
        tested = True
    assert tested

    q = Query().where(reverse=entity_id, schema="Event")
    q = q.where(prop="date", value=2022, comparator="gte")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 3
    q = Query().where(reverse=entity_id, schema="Person")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 0

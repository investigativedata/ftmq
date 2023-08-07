import pytest
from followthemoney import model

from ftmq.exceptions import ValidationError
from ftmq.io import load_proxy
from ftmq.query import Query


def test_proxy_composite():
    data = {"id": "1", "schema": "Thing", "properties": {"name": "Test"}}
    proxy = load_proxy(data)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"default"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test"},
        "datasets": ["test_dataset"],
    }
    proxy = load_proxy(data)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"test_dataset"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test"},
        "datasets": ["test_dataset", "ds2"],
    }
    proxy = load_proxy(data, "another_dataset")
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

    # invalid
    with pytest.raises(ValidationError):
        q = Query().where(schema="Invalid schema")


def test_proxy_filter_property(proxies):
    q = Query().where(prop="jurisdiction", value="eu")
    result = list(filter(q.apply, proxies))
    assert len(result) == 254

    q = Query().where(prop="date", value="2022", operator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3575

    q = Query().where(prop="date", value="2022", operator="gt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3575

    # chained same props as AND
    q = q.where(prop="date", value="2023", operator="lt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 3499

    q = Query().where(prop="date", value=2023, operator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 76

    q = Query().where(prop="date", value=True, operator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975

    q = Query().where(prop="date", value=False, operator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 34975


def test_proxy_filters_combined(proxies):
    q = Query().where(prop="jurisdiction", value="eu")
    q = q.where(schema="Event")
    result = list(filter(q.apply, proxies))
    assert len(result) == 0

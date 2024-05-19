import pytest
from followthemoney import model
from followthemoney.exc import InvalidData

from ftmq.aggregate import aggregate, merge
from ftmq.util import make_proxy


def test_aggregate():
    p1 = make_proxy(
        {"id": "a", "schema": "LegalEntity", "properties": {"name": ["Jane"]}}
    )
    p2 = make_proxy(
        {"id": "a", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
    )
    assert merge(p1, p2).schema.name == "Person"
    p1.schema = model.get("Company")
    with pytest.raises(InvalidData):
        merge(p1, p2)
    assert merge(p1, p2, downgrade=True).schema.name == "LegalEntity"

    p1 = make_proxy(
        {
            "id": "a",
            "schema": "Company",
            "properties": {"name": ["Jane"], "registrationNumber": ["123"]},
        }
    )
    p2 = make_proxy(
        {
            "id": "a",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"], "birthDate": ["2001"]},
        }
    )
    assert merge(p1, p2, downgrade=True).schema.name == "LegalEntity"

    # higher level aggregate function
    with pytest.raises(InvalidData):
        next(aggregate([p1, p2]))

    proxy = next(aggregate([p1, p2], downgrade=True))
    assert proxy.schema.name == "LegalEntity"

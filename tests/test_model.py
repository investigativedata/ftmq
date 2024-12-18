import pytest
from nomenklatura.dataset.catalog import DataCatalog as NKCatalog
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.dataset import Dataset as NKDataset
from nomenklatura.dataset.publisher import DataPublisher as NKPublisher
from nomenklatura.dataset.resource import DataResource as NKResource
from nomenklatura.entity import CompositeEntity
from pydantic import ValidationError

from ftmq.model import Catalog, Dataset, Entity, Publisher, Resource
from ftmq.model.coverage import Coverage
from ftmq.util import make_proxy


def test_model_publisher():
    p = Publisher(name="Test", url="https://example.org/")
    assert p.name == NKPublisher(p.model_dump()).name
    assert str(p.url) == NKPublisher(p.model_dump()).url


def test_model_resource():
    r = Resource(name="entities.ftm.json", url="https://example.com/entities.ftm.json")
    assert r.name == NKResource(r.model_dump()).name
    assert str(r.url) == NKResource(r.model_dump()).url
    assert r.size == NKResource(r.model_dump()).size == 0


def test_model_coverage():
    c = Coverage()
    assert c.frequency == "unknown"
    assert c.frequency == NKCoverage(c.model_dump()).frequency
    c = Coverage(frequency="weekly")
    assert c.frequency == NKCoverage(c.model_dump()).frequency
    with pytest.raises(ValidationError):
        Coverage(frequency="foo")


def test_model_dataset():
    d = Dataset(name="test-dataset")
    assert d.title == "Test-Dataset"
    assert d.prefix == "test-dataset"
    assert d.name == NKDataset(d.model_dump()).name
    assert d.title == NKDataset(d.model_dump()).title

    d = Dataset(name="test-dataset", prefix="td")
    assert d.prefix == "td"

    data = {
        "name": "test",
        "publisher": {"name": "Test publisher", "url": "https://example.org"},
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": "https://example.org/entities.ftm.json",
            }
        ],
        "coverage": {"frequency": "daily"},
    }
    d = Dataset(**data)
    assert d.title == "Test"
    assert d.coverage.frequency == "daily"


def test_model_catalog_full(fixtures_path):
    # ftmq vs. nomenklatura

    catalog = Catalog(datasets=[Dataset(name="test")])
    assert isinstance(catalog.datasets[0], Dataset)
    assert NKDataset(catalog.datasets[0].model_dump())
    assert NKCatalog(NKDataset, catalog.model_dump())
    assert len(catalog.names) == 1
    assert isinstance(catalog.names, set)
    assert "test" in catalog.names

    catalog = Catalog.from_yaml_uri(fixtures_path / "catalog.yml")
    assert catalog.name == "Catalog"
    assert len(catalog.datasets) == 7
    ds = catalog.datasets[0]
    assert ds.name == "eu_transparency_register"
    assert ds.title == "EU Transparency Register"
    # local overwrite:
    assert ds.maintainer.name == "||)·|()"
    assert len(catalog.names) == 7


def test_model_catalog_iterate(fixtures_path):
    catalog = Catalog.from_yaml_uri(fixtures_path / "catalog_small.yml")
    tested = False
    for proxy in catalog.iterate():
        assert isinstance(proxy, CompositeEntity)
        tested = True
        break
    assert tested


def test_model_proxy():
    data = {
        "id": "foo-1",
        "schema": "LegalEntity",
        "properties": {"name": ["Jane Doe"]},
    }
    entity = Entity(**data)
    proxy = make_proxy(data)
    assert entity.to_proxy() == proxy == Entity.from_proxy(proxy).to_proxy()

    data["properties"]["addressEntity"] = "addr"
    address = {
        "id": "addr",
        "schema": "Address",
    }
    adjacents = [make_proxy(address)]
    entity = Entity.from_proxy(make_proxy(data), adjacents=adjacents)
    assert isinstance(entity.properties["addressEntity"][0], Entity)

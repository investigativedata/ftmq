import pytest
from pydantic import ValidationError

from ftmq.model import Catalog, Coverage, Dataset, Publisher, Resource
from ftmq.model.dataset import NKCatalog, NKCoverage, NKDataset, NKPublisher, NKResource


def test_model_publisher():
    p = Publisher(name="Test", url="https://example.org/")
    assert p.name == NKPublisher(p.dict()).name
    assert p.url == NKPublisher(p.dict()).url
    assert isinstance(p.to_nk(), NKPublisher)


def test_model_resource():
    r = Resource(name="entities.ftm.json", url="https://example.com/entities.ftm.json")
    assert r.name == NKResource(r.dict()).name
    assert r.url == NKResource(r.dict()).url
    assert r.size == NKResource(r.dict()).size == 0
    assert isinstance(r.to_nk(), NKResource)


def test_model_coverage():
    c = Coverage()
    assert c.frequency == "unknown"
    assert c.frequency == NKCoverage(c.dict()).frequency
    assert isinstance(c.to_nk(), NKCoverage)
    c = Coverage(frequency="weekly")
    assert c.frequency == NKCoverage(c.dict()).frequency
    with pytest.raises(ValidationError):
        Coverage(frequency="foo")


def test_model_dataset():
    d = Dataset(name="test-dataset")
    assert isinstance(d.to_nk(), NKDataset)
    assert d.title == "Test-Dataset"
    assert d.prefix == "test-dataset"
    assert d.name == NKDataset(d.catalog.to_nk(), d.dict()).name
    assert d.title == NKDataset(d.catalog.to_nk(), d.dict()).title

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
    data = d.to_nk().to_dict()
    assert data["title"] == "Test"


def test_model_catalog_full(fixtures_path):
    # ftmq vs. nomenklatura

    catalog = Catalog()
    assert isinstance(catalog.to_nk(), NKCatalog)

    catalog = Catalog(datasets=[Dataset(name="test")])
    assert isinstance(catalog.to_nk(), NKCatalog)
    assert isinstance(catalog.datasets[0], Dataset)
    assert isinstance(catalog.datasets[0].to_nk(), NKDataset)
    assert len(catalog.names) == 1
    assert isinstance(catalog.names, set)
    assert "test" in catalog.names

    catalog = Catalog.from_path(fixtures_path / "catalog.yml")
    assert catalog.name == "Catalog"
    assert len(catalog.datasets) == 7
    assert catalog.datasets[0].name == "eu_transparency_register"
    assert len(catalog.names) == 7

    catalog = Catalog.from_path(fixtures_path / "catalog_full.yml")
    assert catalog.name == "Test Catalog"
    assert catalog.maintainer.name == "investigraph"
    assert catalog.maintainer.url == "https://investigraph.dev"
    assert len(catalog.datasets) == 1
    assert catalog.datasets[0].name == "eutr"
    assert len(list(catalog.get_datasets())) == 17
    assert len(catalog.names) == 17

    o_catalog = catalog.catalogs[1]
    assert o_catalog.url == "https://opensanctions.org"
    assert o_catalog.maintainer.name == "OpenSanctions"
    assert o_catalog.maintainer.description == "OpenSanctions is cool"

    ftg_catalog = catalog.catalogs[2]
    assert ftg_catalog.datasets[0].name == "pubmed"
    assert ftg_catalog.datasets[0].prefix == "pubmed"


def test_model_catalog_legacy(fixtures_path):
    catalog = Catalog.from_path(fixtures_path / "catalog_legacy.yml")
    assert catalog.name == "Catalog"
    assert len(catalog.datasets) == 7
    assert catalog.datasets[0].name == "eu_transparency_register"


def test_model_catalog_metadata(fixtures_path):
    catalog = Catalog.from_path(fixtures_path / "catalog_full.yml")
    metadata = catalog.metadata()
    assert len(metadata["catalogs"]) == 3
    looped = False
    for c in metadata["catalogs"]:
        assert len(c["datasets"]) == 0
        looped = True
    assert looped

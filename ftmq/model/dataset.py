from collections.abc import Generator
from datetime import datetime
from typing import Any, ForwardRef, Iterable, Literal, Optional, TypeVar

from nomenklatura.dataset.catalog import DataCatalog as NKCatalog
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.dataset import Dataset as NKDataset
from nomenklatura.dataset.publisher import DataPublisher as NKPublisher
from nomenklatura.dataset.resource import DataResource as NKResource
from normality import slugify
from pydantic import AnyUrl, HttpUrl

from ftmq.model.coverage import Coverage
from ftmq.model.mixins import BaseModel, NKModel, RemoteMixin, YamlMixin

Frequencies = Literal[tuple(NKCoverage.FREQUENCIES)]

C = TypeVar("C", bound="Catalog")
DS = TypeVar("DS", bound="Dataset")


class Publisher(NKModel):
    _nk_model = NKPublisher

    name: str
    url: HttpUrl
    description: str | None = None
    country: str | None = None
    country_label: str | None = None
    official: bool = False
    logo_uri: AnyUrl | None = None


class Resource(NKModel):
    _nk_model = NKResource

    name: str
    url: AnyUrl
    title: str | None = None
    checksum: str | None = None
    timestamp: datetime | None = None
    mime_type: str | None = None
    mime_type_label: str | None = None
    size: int | None = 0


class Maintainer(BaseModel, RemoteMixin, YamlMixin):
    """
    this is our own addition
    """

    name: str
    description: str | None = None
    url: HttpUrl | None = None
    logo_uri: HttpUrl | None = None


Catalog = ForwardRef("Catalog")


class Dataset(NKModel):
    _nk_model = NKDataset

    name: str
    prefix: str | None = None
    title: str | None = None
    license: str | None = None
    summary: str | None = None
    description: str | None = None
    url: HttpUrl | None = None
    updated_at: datetime | None = None
    version: str | None = None
    category: str | None = None
    publisher: Publisher | None = None
    coverage: Coverage | None = None
    resources: list[Resource] | None = []

    # own addition
    git_repo: AnyUrl | None = None
    uri: AnyUrl | None = None
    maintainer: Maintainer | None = None
    catalog: Optional[Catalog] = None

    def __init__(self, **data):
        Catalog.update_forward_refs()
        Dataset.update_forward_refs()
        if "include" in data:  # legacy behaviour
            data["uri"] = data.pop("include", None)
        data["updated_at"] = data.get("updated_at") or datetime.utcnow().replace(
            microsecond=0
        )
        data["catalog"] = data.get("catalog") or Catalog().dict()
        super().__init__(**data)
        self.prefix = data.get("prefix") or slugify(self.name)
        self.coverage = self.coverage or Coverage()
        self.title = self.title or self.name.title()

    def to_nk(self):
        return self._nk_model(self.catalog.to_nk(), self.dict())


Dataset.update_forward_refs()


class Catalog(NKModel):
    _nk_model = NKCatalog

    datasets: list[Dataset] | None = []
    updated_at: datetime | None = None

    # own additions
    name: str | None = "default"
    maintainer: Maintainer | None = None
    url: HttpUrl | None = None
    uri: AnyUrl | None = None
    logo_uri: AnyUrl | None = None
    catalogs: list[Catalog] | None = []

    def __init__(self, **data):
        if "name" not in data:
            data["name"] = "Catalog"
        super().__init__(**data)

    def to_nk(self):
        return self._nk_model(NKDataset, self.dict())

    def get(self, name: str) -> Dataset | None:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset

    def get_datasets(self) -> Generator[Dataset, None, None]:
        yield from self.datasets
        for catalog in self.catalogs:
            yield from catalog.datasets

    def get_scope(self) -> NKDataset:
        catalog = self.to_nk()
        return NKDataset(
            catalog,
            {
                "name": slugify(self.name),
                "title": self.name.title(),
                "children": catalog.names,
            },
        )

    def metadata(self) -> dict[str, Any]:
        catalog = self.copy()
        catalog.datasets = []
        catalog.catalogs = [c.metadata() for c in self.catalogs]
        return catalog.dict()

    @property
    def names(self) -> set:
        names = set()
        names.update(self.to_nk().names)
        for catalog in self.catalogs:
            names.update(catalog.names)
        return names

    @classmethod
    def from_names(cls, names: Iterable[set]) -> Catalog:
        return cls(datasets=[Dataset(name=n) for n in names])


Catalog.update_forward_refs()

from functools import cache
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlparse

from nomenklatura import store as nk
from nomenklatura.dataset import DS, DefaultDataset
from nomenklatura.db import ensure_tx
from nomenklatura.resolver import Resolver

from ftmq.aleph import AlephStore as _AlephStore
from ftmq.aleph import AlephView, parse_uri
from ftmq.model.coverage import Collector, Coverage
from ftmq.model.dataset import C, Dataset
from ftmq.query import Q
from ftmq.settings import STORE_URI
from ftmq.types import CE, CEGenerator, PathLike


class Store(nk.Store):
    def __init__(
        self, catalog: C | None = None, dataset: Dataset | None = None, *args, **kwargs
    ) -> None:
        if catalog is not None:
            dataset = catalog.get_scope()
        elif dataset is not None:
            if isinstance(dataset, str):
                dataset = Dataset(name=dataset)
            dataset = dataset.to_nk()
        else:
            dataset = DefaultDataset
        super().__init__(dataset=dataset, resolver=Resolver(), *args, **kwargs)

    def iterate(self) -> CEGenerator:
        view = self.default_view()
        yield from view.entities()


class CoverageView(nk.base.View):
    def coverage(self, query: Q | None = None) -> Coverage:
        c = Coverage()
        c.apply(self.entities(query))
        return c


class MemoryQueryView(nk.memory.MemoryView, CoverageView):
    def entities(self, query: Q | None = None) -> CEGenerator:
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()


class LevelDBQueryView(nk.level.LevelDBView, CoverageView):
    def entities(self, query: Q | None = None) -> CEGenerator:
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()


class SqlQueryView(nk.sql.SqlView):
    def entities(self, query: Q | None = None) -> CEGenerator:
        if query:
            yield from self.store._iterate(query.sql.statements)
        else:
            view = self.store.view(self.scope)
            yield from view.entities()

    def coverage(self, query: Q | None = None) -> Coverage:
        c = Collector()
        with ensure_tx(self.store.engine.connect()) as tx:
            for schema, count in tx.execute(query.sql.schemata):
                c.schemata[schema] = count
            for country, _ in tx.execute(query.sql.countries):
                if country is not None:
                    c.countries.add(country)
            coverage = c.export()
            coverage.entities = tx.execute(query.sql.count).scalar()
        return coverage


class AlephQueryView(AlephView, CoverageView):
    def entities(self, query: Q | None = None) -> CEGenerator:
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()


class MemoryStore(Store, nk.SimpleMemoryStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return MemoryQueryView(self, scope, external=external)


class LevelDBStore(Store, nk.LevelDBStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return LevelDBQueryView(self, scope, external=external)


class SqlStore(Store, nk.SqlStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return SqlQueryView(self, scope, external=external)


class AlephStore(Store, _AlephStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return AlephQueryView(self, scope, external=external)

    @classmethod
    def from_uri(
        cls, uri: str, dataset: Dataset | str | None = None, catalog: C | None = None
    ) -> nk.Store[DS, CE]:
        host, api_key, foreign_id = parse_uri(uri)
        if dataset is None and foreign_id is not None:
            dataset = foreign_id
        if dataset is not None:
            if isinstance(dataset, str):
                dataset = Dataset(name=dataset)

        return cls(catalog, dataset, host=host, api_key=api_key)


S = TypeVar("S", bound=Store)


@cache
def get_store(
    uri: PathLike | None = STORE_URI,
    catalog: C | None = None,
    dataset: Dataset | str | None = None,
) -> Store:
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(catalog, dataset)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        return LevelDBStore(catalog, dataset, path=path)
    if "sql" in parsed.scheme:
        return SqlStore(catalog, dataset, uri=uri)
    if "aleph" in parsed.scheme:
        return AlephStore.from_uri(uri, catalog=catalog, dataset=dataset)
    raise NotImplementedError(uri)


__all__ = ["get_store", "S", "LevelDBStore", "MemoryStore", "SqlStore"]
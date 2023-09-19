from collections import defaultdict
from functools import cache
from pathlib import Path
from typing import Iterable, TypeVar
from urllib.parse import urlparse

from nomenklatura import store as nk
from nomenklatura.dataset import DS, DefaultDataset
from nomenklatura.db import get_metadata
from nomenklatura.resolver import Resolver
from sqlalchemy import select

from ftmq.aggregations import AggregatorResult
from ftmq.aleph import AlephStore as _AlephStore
from ftmq.aleph import AlephView, parse_uri
from ftmq.exceptions import ValidationError
from ftmq.model.coverage import Collector, Coverage
from ftmq.model.dataset import C, Catalog, Dataset
from ftmq.query import Q, Query
from ftmq.settings import STORE_URI
from ftmq.types import CE, CEGenerator, PathLike


class Store(nk.Store):
    def __init__(
        self,
        catalog: C | None = None,
        dataset: Dataset | None = None,
        resolver: Resolver | None = None,
        *args,
        **kwargs,
    ) -> None:
        if dataset is not None:
            if isinstance(dataset, str):
                dataset = Dataset(name=dataset)
            dataset = dataset.to_nk()
        elif catalog is not None:
            dataset = catalog.get_scope()
        else:
            dataset = DefaultDataset
        super().__init__(
            dataset=dataset, resolver=resolver or Resolver(), *args, **kwargs
        )
        self.cache = {}

    def iterate(self) -> CEGenerator:
        view = self.default_view()
        yield from view.entities()

    def get_catalog(self) -> C:
        # return implicit catalog computed from current datasets in store
        raise NotImplementedError


class View(nk.base.View):
    def entities(self, query: Q | None = None) -> CEGenerator:
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()

    def get_adjacents(self, proxies: Iterable[CE]) -> CEGenerator:
        seen = set()
        for proxy in proxies:
            for _, adjacent in self.get_adjacent(proxy):
                if adjacent.id not in seen:
                    seen.add(adjacent.id)
                    yield adjacent

    def coverage(self, query: Q | None = None) -> Coverage:
        c = Coverage()
        c.apply(self.entities(query))
        return c

    def aggregations(self, query: Q) -> AggregatorResult | None:
        if not query.aggregations:
            return
        aggregator = query.apply_aggregations(self.entities(query))
        return dict(aggregator.result)


class MemoryQueryView(View, nk.memory.MemoryView):
    pass


class LevelDBQueryView(View, nk.level.LevelDBView):
    pass


class AlephQueryView(View, AlephView):
    pass


class SQLQueryView(View, nk.sql.SQLView):
    def ensure_scoped_query(self, query: Q) -> Q:
        if not query.datasets:
            return query.where(dataset__in=self.dataset_names)
        if query.dataset_names - self.dataset_names:
            raise ValidationError("Query datasets outside view scope")
        return query

    def entities(self, query: Q | None = None) -> CEGenerator:
        if query:
            query = self.ensure_scoped_query(query)
            yield from self.store._iterate(query.sql.statements)
        else:
            view = self.store.view(self.scope)
            yield from view.entities()

    def coverage(self, query: Q | None = None) -> Coverage:
        query = self.ensure_scoped_query(query or Query())
        c = Collector()
        for schema, count in self.store._execute(query.sql.schemata, stream=False):
            c.schemata[schema] = count
        for country, count in self.store._execute(query.sql.countries, stream=False):
            if country is not None:
                c.countries[country] = count
        coverage = c.export()
        for start, end in self.store._execute(query.sql.dates, stream=False):
            coverage.start = start
            coverage.end = end

        for res in self.store._execute(query.sql.count, stream=False):
            for count in res:
                coverage.entities = count
                break
        return coverage

    def aggregations(self, query: Q) -> AggregatorResult | None:
        if not query.aggregations:
            return
        query = self.ensure_scoped_query(query)
        res: AggregatorResult = defaultdict(dict)
        for prop, func, value in self.store._execute(
            query.sql.aggregations, stream=False
        ):
            res[func][prop] = value
        return res


class MemoryStore(Store, nk.SimpleMemoryStore):
    def get_catalog(self) -> C:
        return Catalog.from_names(self.entities.keys())

    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return MemoryQueryView(self, scope, external=external)


class LevelDBStore(Store, nk.LevelDBStore):
    def get_catalog(self) -> C:
        names: set[str] = set()
        with self.db.iterator(prefix=b"e:", include_value=False) as it:
            for k in it:
                _, _, dataset = k.decode("utf-8").split(":", 2)
                names.add(dataset)
        return Catalog.from_names(names)

    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return LevelDBQueryView(self, scope, external=external)


class SQLStore(Store, nk.SQLStore):
    def get_catalog(self) -> C:
        q = select(self.table.c.dataset).distinct()
        names: set[str] = set()
        for row in self._execute(q, stream=False):
            names.add(row[0])
        return Catalog.from_names(names)

    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return SQLQueryView(self, scope, external=external)


class AlephStore(Store, _AlephStore):
    def get_catalog(self) -> C:
        # FIXME
        # api.filter_collections("*")
        return Catalog.from_names(DefaultDataset.leaf_names)

    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return AlephQueryView(self, scope, external=external)

    @classmethod
    def from_uri(
        cls,
        uri: str,
        dataset: Dataset | str | None = None,
        catalog: C | None = None,
        resolver: Resolver | None = None,
    ) -> nk.Store[DS, CE]:
        host, api_key, foreign_id = parse_uri(uri)
        if dataset is None and foreign_id is not None:
            dataset = foreign_id
        if dataset is not None:
            if isinstance(dataset, str):
                dataset = Dataset(name=dataset)

        return cls(catalog, dataset, resolver=resolver, host=host, api_key=api_key)


S = TypeVar("S", bound=Store)


@cache
def get_store(
    uri: PathLike | None = STORE_URI,
    catalog: C | None = None,
    dataset: Dataset | str | None = None,
    resolver: Resolver | str | None = None,
) -> Store:
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    if isinstance(resolver, (str, Path)):
        resolver = Resolver.load(resolver)
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(catalog, dataset, resolver=resolver)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        return LevelDBStore(catalog, dataset, path=path, resolver=resolver)
    if "sql" in parsed.scheme:
        get_metadata.cache_clear()
        return SQLStore(catalog, dataset, uri=uri, resolver=resolver)
    if "aleph" in parsed.scheme:
        return AlephStore.from_uri(
            uri, catalog=catalog, dataset=dataset, resolver=resolver
        )
    raise NotImplementedError(uri)


__all__ = ["get_store", "S", "LevelDBStore", "MemoryStore", "SQLStore"]

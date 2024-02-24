import os
from collections import defaultdict
from datetime import date

from anystore.util import clean_dict
from nomenklatura import store as nk
from nomenklatura.dataset import DS
from sqlalchemy import select

from ftmq.aggregations import AggregatorResult
from ftmq.enums import Fields
from ftmq.exceptions import ValidationError
from ftmq.model.coverage import Collector, DatasetStats
from ftmq.model.dataset import Catalog
from ftmq.query import Q, Query
from ftmq.store.base import Store, View
from ftmq.types import CEGenerator

MAX_SQL_AGG_GROUPS = int(os.environ.get("MAX_SQL_AGG_GROUPS", 10))


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

    def stats(self, query: Q | None = None) -> DatasetStats:
        query = self.ensure_scoped_query(query or Query())
        key = f"stats-{hash(query)}"
        if key in self._cache:
            return self._cache[key]

        c = Collector()
        for schema, count in self.store._execute(query.sql.things, stream=False):
            c.things[schema] = count
        for schema, count in self.store._execute(query.sql.intervals, stream=False):
            c.intervals[schema] = count
        for country, count in self.store._execute(
            query.sql.things_countries, stream=False
        ):
            if country is not None:
                c.things_countries[country] = count
        for country, count in self.store._execute(
            query.sql.intervals_countries, stream=False
        ):
            if country is not None:
                c.intervals_countries[country] = count
        stats = c.export()
        for start, end in self.store._execute(query.sql.date_range, stream=False):
            if start is not None:
                stats.coverage.start = date.fromisoformat(start)
            if end is not None:
                stats.coverage.end = date.fromisoformat(end)
            break

        for res in self.store._execute(query.sql.countries, stream=False):
            stats.coverage.countries.append(res)

        for res in self.store._execute(query.sql.count, stream=False):
            for count in res:
                stats.entity_count = count
                break
        self._cache[key] = stats
        return stats

    def aggregations(self, query: Q) -> AggregatorResult | None:
        if not query.aggregations:
            return
        query = self.ensure_scoped_query(query)
        key = f"agg-{hash(query)}"
        if key in self._cache:
            return self._cache[key]
        res: AggregatorResult = defaultdict(dict)

        for prop, func, value in self.store._execute(
            query.sql.aggregations, stream=False
        ):
            res[func][prop] = value

        if query.sql.group_props:
            res["groups"] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
            for prop in query.sql.group_props:
                if prop == Fields.year:
                    start, end = self.stats(query).coverage.years
                    if start or end:
                        groups = range(start or end, (end or start) + 1)
                    else:
                        groups = []
                else:
                    groups = [
                        r[0]
                        for r in self.store._execute(
                            query.sql.get_group_counts(prop, limit=MAX_SQL_AGG_GROUPS),
                            stream=False,
                        )
                    ]
                for group in groups:
                    for agg_prop, func, value in self.store._execute(
                        query.sql.get_group_aggregations(prop, group), stream=False
                    ):
                        res["groups"][prop][func][agg_prop][group] = value
        res = clean_dict(res)
        self._cache[key] = res
        return res


class SQLStore(Store, nk.SQLStore):
    def get_catalog(self) -> Catalog:
        q = select(self.table.c.dataset).distinct()
        names: set[str] = set()
        for row in self._execute(q, stream=False):
            names.add(row[0])
        return Catalog.from_names(names)

    def query(self, scope: DS | None = None, external: bool = False) -> SQLQueryView:
        scope = scope or self.dataset
        return SQLQueryView(self, scope, external=external)

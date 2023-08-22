from collections.abc import Iterable
from itertools import islice
from typing import Any, TypedDict, TypeVar

from banal import ensure_list, is_listish, is_mapping
from nomenklatura.entity import CE

from ftmq.aggregations import Aggregation, Aggregator
from ftmq.enums import Aggregations, Operators, Properties
from ftmq.exceptions import ValidationError
from ftmq.filters import (
    Dataset,
    DatasetFilter,
    F,
    Property,
    PropertyFilter,
    Schema,
    SchemaFilter,
    Value,
)
from ftmq.sql import Sql
from ftmq.types import CEGenerator
from ftmq.util import parse_unknown_filters

Q = TypeVar("Q", bound="Query")
L = TypeVar("L", bound="Lookup")
Slice = TypeVar("Slice", bound=slice)


class Lookup(TypedDict):
    dataset: Dataset | str | None = None
    schema: Schema | str | None = None
    prop: Property | str | None = None
    value: Value | None = None
    operator: Operators = None


class Sort:
    values: tuple[str] | None = None
    ascending: bool | None = True

    def __init__(self, values: Iterable[str], ascending: bool | None = True) -> None:
        self.values = tuple(values)
        self.ascending = ascending

    def apply(self, proxy: CE) -> tuple[str]:
        values = tuple()
        for v in self.values:
            p_values = proxy.get(v, quiet=True)
            if p_values is not None:
                values = values + (tuple(p_values))
        return values

    def apply_iter(self, proxies: CEGenerator) -> CEGenerator:
        yield from sorted(
            proxies, key=lambda x: self.apply(x), reverse=not self.ascending
        )

    def serialize(self) -> list[str]:
        if self.ascending:
            return list(self.values)
        return [f"-{v}" for v in self.values]


class Query:
    filters: set[F] = set()
    aggregations: set[Aggregation] = set()
    aggregator: Aggregator | None = None
    sort: Sort | None = None
    slice: Slice | None = None

    def __init__(
        self,
        filters: Iterable[F] | None = None,
        aggregations: Iterable[Aggregation] | None = None,
        aggregator: Aggregator | None = None,
        sort: Sort | None = None,
        slice: Slice | None = None,
    ):
        self.filters = set(ensure_list(filters))
        self.aggregations = set(ensure_list(aggregations))
        self.aggregator = aggregator
        self.sort = sort
        self.slice = slice

    def __getitem__(self, value: Any) -> Any:
        # slicing
        if isinstance(value, int):
            if value < 0:
                raise ValidationError(f"Invalid slicing: `{value}`")
            return self._chain(slice=slice(value, value + 1))
        if isinstance(value, slice):
            if value.step is not None:
                raise ValidationError(f"Invalid slicing: `{value}`")
            return self._chain(slice=value)
        raise NotImplementedError

    def __bool__(self) -> bool:
        """
        Detect if we have anything to do
        """
        return bool(self.to_dict())

    def _chain(self, **kwargs):
        # merge current state
        new_kwargs = self.__dict__.copy()
        for key, new_value in kwargs.items():
            old_value = new_kwargs[key]
            if old_value is None:
                new_kwargs[key] = new_value
            # "remove" old value:
            elif new_value is None:
                new_kwargs[key] = None
            # overwrite order by
            elif key == "sort":
                new_kwargs[key] = new_value
            # combine iterables and dicts
            elif is_listish(old_value):
                new_kwargs[key] = sorted(set(old_value) | set(new_value))
            elif is_mapping(old_value):
                new_kwargs[key] = {**old_value, **new_value}
            else:  # replace
                new_kwargs[key] = new_value
        return self.__class__(**new_kwargs)

    @property
    def lookups(self) -> dict[str, Any]:
        data = {}
        for fi in self.filters:
            for k, v in fi.to_dict().items():
                if k in data:
                    data[k] = list(sorted(ensure_list(data[k]) + [v]))
                else:
                    data[k] = v
        return data

    @property
    def limit(self) -> int | None:
        if self.slice is None:
            return None
        if self.slice.start and self.slice.stop:
            return self.slice.stop - self.slice.start
        return self.slice.stop

    @property
    def offset(self) -> int | None:
        return self.slice.start if self.slice else None

    @property
    def sql(self) -> Sql:
        return Sql(self)

    @property
    def datasets(self) -> set[DatasetFilter]:
        return {f for f in self.filters if isinstance(f, DatasetFilter)}

    @property
    def dataset_names(self) -> set[str]:
        return {str(f) for f in self.datasets}

    @property
    def schemata(self) -> set[SchemaFilter]:
        return {f for f in self.filters if isinstance(f, SchemaFilter)}

    @property
    def properties(self) -> set[PropertyFilter]:
        return {f for f in self.filters if isinstance(f, PropertyFilter)}

    def to_dict(self) -> dict[str, Any]:
        data = self.lookups
        if self.sort:
            data["order_by"] = self.sort.serialize()
        if self.slice:
            data["limit"] = self.limit
            data["offset"] = self.offset
        if self.aggregations:
            data["aggregations"] = self.get_aggregator().to_dict()
        return data

    def where(self, **lookup: Lookup) -> Q:
        include_descendants = lookup.pop("include_descendants", False)
        include_matchable = lookup.pop("include_matchable", False)
        dataset = lookup.pop("dataset", [])
        if dataset is None:  # reset filters
            for f in self.datasets:
                self.filters.discard(f)
        for name in ensure_list(dataset):
            self.filters.add(DatasetFilter(name))
        schema = lookup.pop("schema", [])
        if schema is None:  # reset filters
            for f in self.schemata:
                self.filters.discard(f)
        for name in ensure_list(schema):
            self.filters.add(
                SchemaFilter(
                    name,
                    include_descendants=include_descendants,
                    include_matchable=include_matchable,
                )
            )
        if "prop" in lookup:
            if "value" not in lookup:
                raise ValidationError("No lookup value specified")
            f = PropertyFilter(
                lookup.pop("prop"), lookup.pop("value"), lookup.pop("operator", None)
            )
            self.filters.discard(f)  # replace existing property filter with updated one
            self.filters.add(f)

        # parse arbitrary `date_gte=2023` stuff
        for key, val in lookup.items():
            for prop, value, operator in parse_unknown_filters((key, val)):
                f = PropertyFilter(prop, value, operator)
                self.filters.discard(
                    f
                )  # replace existing property filter with updated one
                self.filters.add(f)

        return self._chain()

    def order_by(self, *values: Iterable[str], ascending: bool | None = True) -> Q:
        self.sort = Sort(values=values, ascending=ascending)
        return self._chain()

    def aggregate(self, func: Aggregations, *props: Properties) -> Q:
        for prop in props:
            self.aggregations.add(Aggregation(func=func, prop=prop))
        return self._chain()

    def get_aggregator(self) -> Aggregator:
        return Aggregator(aggregations=self.aggregations)

    def apply(self, proxy: CE) -> bool:
        if not self.filters:
            return True
        return all(f.apply(proxy) for f in self.filters)

    def apply_iter(self, proxies: CEGenerator) -> CEGenerator:
        """
        apply a `Query` to a generator of proxies and return a generator of filtered proxies
        """
        if not self:
            yield from proxies
            return

        proxies = (p for p in proxies if self.apply(p))
        if self.sort:
            proxies = self.sort.apply_iter(proxies)
        if self.slice:
            proxies = islice(
                proxies, self.slice.start, self.slice.stop, self.slice.step
            )
        if self.aggregations:
            self.aggregator = self.get_aggregator()
            proxies = self.aggregator.apply(proxies)
        yield from proxies

    def apply_aggregations(self, proxies: CEGenerator) -> Aggregator:
        aggregator = self.get_aggregator()
        [x for x in aggregator.apply(proxies)]
        return aggregator

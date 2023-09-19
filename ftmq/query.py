from collections.abc import Iterable
from itertools import islice
from typing import Any, TypeVar

from banal import ensure_list, is_listish, is_mapping
from nomenklatura.entity import CE

from ftmq.aggregations import Aggregation, Aggregator
from ftmq.enums import Aggregations, Properties
from ftmq.exceptions import ValidationError
from ftmq.filters import (
    FILTERS,
    DatasetFilter,
    F,
    PropertyFilter,
    ReverseFilter,
    SchemaFilter,
)
from ftmq.sql import Sql
from ftmq.types import CEGenerator
from ftmq.util import parse_comparator, parse_unknown_filters

Q = TypeVar("Q", bound="Query")
Slice = TypeVar("Slice", bound=slice)


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
                current = data.get(k)
                if is_listish(current):
                    data[k].append(v)
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
        names = set()
        for f in self.datasets:
            names.update(ensure_list(f.value))
        return names

    @property
    def schemata(self) -> set[SchemaFilter]:
        return {f for f in self.filters if isinstance(f, SchemaFilter)}

    @property
    def reversed(self) -> set[ReverseFilter]:
        return {f for f in self.filters if isinstance(f, ReverseFilter)}

    @property
    def properties(self) -> set[PropertyFilter]:
        return {f for f in self.filters if isinstance(f, PropertyFilter)}

    def discard(self, f_cls: F) -> None:
        filters = list(self.filters)
        for f in filters:
            if isinstance(f, f_cls):
                self.filters.discard(f)

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

    def where(self, **lookup: dict[str, Any]) -> Q:
        include_descendants = lookup.pop("include_descendants", False)
        include_matchable = lookup.pop("include_matchable", False)
        prop = lookup.pop("prop", None)
        value = lookup.pop("value", None)
        comparator = lookup.pop("comparator", None)
        if prop is not None:
            if value is None:
                raise ValidationError("No lookup value specified")
            f = PropertyFilter(prop, value, comparator)
            self.filters.discard(f)  # replace existing property filter with updated one
            self.filters.add(f)

        properties: dict[str, Any] = {}
        for key, value in lookup.items():
            meta = False
            for f_key, f in FILTERS.items():
                if key.startswith(f_key):
                    if value is None:
                        self.discard(f)
                    else:
                        key, comparator = parse_comparator(key)
                        kwargs = {}
                        if key == "schema":
                            kwargs = {
                                "include_matchable": include_matchable,
                                "include_descendants": include_descendants,
                            }
                        self.filters.add(f(value, comparator, **kwargs))
                    meta = True
                    break
            if not meta:
                properties[key] = value

        # parse arbitrary `date_gte=2023` stuff
        for key, val in properties.items():
            for prop, value, comparator in parse_unknown_filters((key, val)):
                f = PropertyFilter(prop, value, comparator)
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

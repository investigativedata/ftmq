from collections.abc import Iterable
from itertools import islice
from typing import Any, TypeVar

from banal import ensure_list, is_listish, is_mapping
from nomenklatura.entity import CE

from ftmq.aggregations import Aggregation, Aggregator
from ftmq.enums import Aggregations, Comparators, Properties
from ftmq.exceptions import ValidationError
from ftmq.filters import (
    FILTERS,
    DatasetFilter,
    F,
    IdFilter,
    PropertyFilter,
    ReverseFilter,
    SchemaFilter,
)
from ftmq.sql import Sql
from ftmq.types import CEGenerator
from ftmq.util import (
    parse_comparator,
    parse_unknown_filters,
    prop_is_numeric,
    to_numeric,
)

Q = TypeVar("Q", bound="Query")
Slice = TypeVar("Slice", bound=slice)


class Sort:
    def __init__(self, values: Iterable[str], ascending: bool | None = True) -> None:
        self.values = tuple(values)
        self.ascending = ascending

    def apply(self, proxy: CE) -> tuple[str]:
        values = tuple()
        for v in self.values:
            p_values = proxy.get(v, quiet=True) or []
            if prop_is_numeric(proxy.schema, v):
                p_values = map(to_numeric, p_values)
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
    DEFAULT_SEARCH_PROPS = (
        Properties["name"],
        Properties["firstName"],
        Properties["middleName"],
        Properties["lastName"],
    )

    def __init__(
        self,
        filters: Iterable[F] | None = None,
        search_filters: Iterable[F] | None = None,
        aggregations: Iterable[Aggregation] | None = None,
        aggregator: Aggregator | None = None,
        sort: Sort | None = None,
        slice: Slice | None = None,
    ):
        self.filters = set(ensure_list(filters))
        self.search_filters = set(ensure_list(search_filters))
        self.aggregations = set(ensure_list(aggregations))
        self.aggregator = aggregator
        self.sort = sort
        self.slice = slice

    def __getitem__(self, value: Any) -> Q:
        """
        Implement list-like slicing. No negative values allowed.

        Examples:
            >>> q[1]
            # 2nd element (0-index)
            >>> q[:10]
            # first 10 elements
            >>> q[10:20]
            # next 10 elements

        Returns:
            The updated `Query` instance
        """
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
        Detect if any filter, ordering or slicing is defined

        Examples:
            >>> bool(Query())
            False
            >>> bool(Query().where(dataset="my_dataset"))
            True
        """
        return bool(self.to_dict())

    def __hash__(self) -> int:
        """
        Generate a unique key of the current state, useful for caching
        """
        return hash(repr(self.to_dict()))

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

    def _get_lookups(self, filters: set[F]) -> dict[str, Any]:
        data = {}
        for fi in filters:
            for k, v in fi.to_dict().items():
                current = data.get(k)
                if is_listish(current):
                    data[k].append(v)
                else:
                    data[k] = v
        return data

    @property
    def lookups(self) -> dict[str, Any]:
        """
        The current filter lookups as dictionary
        """
        return self._get_lookups(self.filters)

    @property
    def search_lookups(self) -> dict[str, Any]:
        """
        The current search lookups as dictionary
        """
        return self._get_lookups(self.search_filters)

    @property
    def limit(self) -> int | None:
        """
        The current limit (inferred from a slice)
        """
        if self.slice is None:
            return None
        if self.slice.start and self.slice.stop:
            return self.slice.stop - self.slice.start
        return self.slice.stop

    @property
    def offset(self) -> int | None:
        """
        The current offset (inferred from a slice)
        """
        return self.slice.start if self.slice else None

    @property
    def sql(self) -> Sql:
        """
        An object of this query used for sql interfaces
        """
        return Sql(self)

    @property
    def ids(self) -> set[IdFilter]:
        """
        The current id filters
        """
        return {f for f in self.filters if isinstance(f, IdFilter)}

    @property
    def datasets(self) -> set[DatasetFilter]:
        """
        The current dataset filters
        """
        return {f for f in self.filters if isinstance(f, DatasetFilter)}

    @property
    def dataset_names(self) -> set[str]:
        """
        The names of the current filtered datasets
        """
        names = set()
        for f in self.datasets:
            names.update(ensure_list(f.value))
        return names

    @property
    def schemata(self) -> set[SchemaFilter]:
        """
        The current schema filters
        """
        return {f for f in self.filters if isinstance(f, SchemaFilter)}

    @property
    def schemata_names(self) -> set[str]:
        """
        The names of the current filtered schemas
        """
        names = set()
        for f in self.schemata:
            names.update(ensure_list(f.value))
        return names

    @property
    def countries(self) -> set[str]:
        """
        The current filtered countries
        """
        names = set()
        for f in self.properties:
            if f.key == "country":
                names.update(ensure_list(f.value))
        return names

    @property
    def reversed(self) -> set[ReverseFilter]:
        """
        The current reverse lookup filters
        """
        return {f for f in self.filters if isinstance(f, ReverseFilter)}

    @property
    def properties(self) -> set[PropertyFilter]:
        """
        The current property lookup filters
        """
        return {f for f in self.filters if isinstance(f, PropertyFilter)}

    def discard(self, f_cls: F) -> None:
        filters = list(self.filters)
        for f in filters:
            if isinstance(f, f_cls):
                self.filters.discard(f)

    def to_dict(self) -> dict[str, Any]:
        """
        Dictionary representation of the current object

        Example:
            ```python
            q = Query().where(dataset__in=["d1", "d2"])
            assert q.to_dict() == {"dataset__in": {"d1", "d2"}}
            q = q.where(schema="Event").where(schema__in=["Person", "Organization"])
            assert q.to_dict() == {
                    "dataset__in": {"d1", "d2"},
                    "schema": "Event",
                    "schema__in": {"Organization", "Person"},
                }
            ```
        """
        data = self.lookups
        search_data = self.search_lookups
        if search_data:
            data["search"] = search_data
        if self.sort:
            data["order_by"] = self.sort.serialize()
        if self.slice:
            data["limit"] = self.limit
            data["offset"] = self.offset
        if self.aggregations:
            data["aggregations"] = self.get_aggregator().to_dict()
        return data

    def where(self, **lookup: Any) -> Q:
        """
        Add another lookup to the current `Query` instance.

        Example:
            ```python
            q = Query().where(dataset="my_dataset")
            q = q.where(schema="Payment")
            q = q.where(date__gte="2024-10", date__lt="2024-11")
            q = q.order_by("amountEur", ascending=False)
            ```

        Args:
            **lookup: A dataset lookup `dataset="my_dataset"`
            **lookup: A schema lookup `schema="Person"`
            **lookup: `include_descendants=True`: Include schema descendants for
                given schema lookup
            **lookup: `include_matchable=True`: Include matchable schema for
                given schema lookup
            **lookup: A property=value lookup (with optional comparators):
                `name__startswith="Ja"`

        Returns:
            The updated `Query` instance
        """
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

        # parse arbitrary `date__gte=2023` stuff
        for key, val in properties.items():
            for prop, value, comparator in parse_unknown_filters((key, val)):
                f = PropertyFilter(prop, value, comparator)
                self.filters.discard(
                    f
                )  # replace existing property filter with updated one
                self.filters.add(f)

        return self._chain()

    def search(self, q: str, props: Iterable[Properties | str] = None) -> Q:
        # reset existing search
        self.search_filters: set[F] = set()
        props = props or self.DEFAULT_SEARCH_PROPS
        for prop in props:
            self.search_filters.add(PropertyFilter(prop, q, Comparators.ilike))
        return self._chain()

    def order_by(self, *values: Iterable[str], ascending: bool | None = True) -> Q:
        """
        Add or update the current sorting.

        Args:
            *values: Fields to order by
            ascending: Ascending or descending

        Returns:
            The updated `Query` instance.
        """
        self.sort = Sort(values=values, ascending=ascending)
        return self._chain()

    def aggregate(
        self,
        func: Aggregations,
        *props: Properties,
        groups: Properties | list[Properties] | None = None,
    ) -> Q:
        for prop in props:
            self.aggregations.add(
                Aggregation(func=func, prop=prop, group_props=ensure_list(groups))
            )
        return self._chain()

    def get_aggregator(self) -> Aggregator:
        return Aggregator(aggregations=self.aggregations)

    def apply_filter(self, proxy: CE) -> bool:
        if not self.filters:
            return True
        return all(f.apply(proxy) for f in self.filters)

    def apply_search(self, proxy: CE) -> bool:
        if not self.search_filters:
            return True
        return any(f.apply(proxy) for f in self.search_filters)

    def apply(self, proxy: CE) -> bool:
        """
        Test if a proxy matches the current `Query` instance.
        """
        if self.apply_filter(proxy):
            return self.apply_search(proxy)
        return False

    def apply_iter(self, proxies: CEGenerator) -> CEGenerator:
        """
        Apply the current `Query` instance to a generator of proxies and return
        a generator of filtered proxies

        Example:
            ```python
            proxies = [...]
            q = Query().where(dataset="my_dataset", schema="Company")
            for proxy in q.apply_iter(proxies):
                assert proxy.schema.name == "Company"
            ```

        Yields:
            A generator of `nomenklatura.entity.CompositeEntity`
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

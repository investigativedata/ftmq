from collections.abc import Iterable
from itertools import islice
from typing import Any, TypedDict, TypeVar

from banal import ensure_list, is_listish, is_mapping
from nomenklatura.entity import CE

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
from ftmq.types import CEGenerator

Q = TypeVar("Q", bound="Query")
L = TypeVar("L", bound="Lookup")
Slice = TypeVar("Slice", bound=slice)


class Lookup(TypedDict):
    dataset: Dataset | str | None = None
    schema: Schema | str | None = None
    prop: Property | str | None = None
    value: Value | None = None


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

    def apply_iter(self, proxies: CE) -> CEGenerator:
        yield from sorted(
            proxies, key=lambda x: self.apply(x), reverse=not self.ascending
        )

    def serialize(self) -> list[str]:
        if self.ascending:
            return list(self.values)
        return [f"-{v}" for v in self.values]


class Query:
    WHERE_KWARGS = {
        "dataset",
        "schema",
        "prop",
        "value",
        "operator",
        "include_descendants",
        "include_matchable",
    }
    filters: set[F] = set()
    order_by: Sort | None = None
    slice: Slice | None = None

    def __init__(
        self,
        filters: Iterable[F] | None = None,
        order_by: Sort | None = None,
        slice: Slice | None = None,
    ):
        self.filters = set(ensure_list(filters))
        self.order_by = order_by
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

    def to_dict(self) -> dict[str, Any]:
        data = {}
        for fi in self.filters:
            data.update(fi.to_dict())
        if self.order_by:
            data["order_by"] = self.order_by.serialize()
        if self.slice:
            data["slice"] = [self.slice.start, self.slice.stop, self.slice.step]
        return data

    def where(self, **lookup: Lookup) -> Q:
        rest = set(lookup.keys()) - self.WHERE_KWARGS
        if rest:
            raise ValidationError(f"Unexpected lookup: `{rest}`")
        if "dataset" in lookup:
            self.filters.add(DatasetFilter(lookup["dataset"]))
        if "schema" in lookup:
            self.filters.add(
                SchemaFilter(
                    lookup["schema"],
                    include_descendants=lookup.pop("include_descendants", False),
                    include_matchable=lookup.pop("include_matchable", False),
                )
            )
        if "prop" in lookup:
            if "value" not in lookup:
                raise ValidationError("No lookup value specified")
            f = PropertyFilter(
                lookup["prop"], lookup["value"], lookup.pop("operator", None)
            )
            self.filters.discard(f)  # replace existing property filter with updated one
            self.filters.add(f)
        return self._chain()

    def sort(self, *values: Iterable[str], ascending: bool | None = True) -> Q:
        self.order_by = Sort(values=values, ascending=ascending)
        return self._chain()

    def apply(self, proxy: CE) -> bool:
        if not self.filters:
            return True
        return all(f.apply(proxy) for f in self.filters)

    def apply_iter(self, proxies: CEGenerator) -> CEGenerator:
        """
        apply a `Query` to a generator of proxies and return a generator of filtered proxies
        """
        proxies = (p for p in proxies if self.apply(p))
        if self.order_by:
            proxies = self.order_by.apply_iter(proxies)
        if self.slice:
            proxies = islice(
                proxies, self.slice.start, self.slice.stop, self.slice.step
            )
        yield from proxies

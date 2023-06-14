from typing import Iterable, Optional, TypedDict, TypeVar

from nomenklatura.entity import CE

from .filters import (
    Dataset,
    DatasetFilter,
    F,
    Property,
    PropertyFilter,
    Schema,
    SchemaFilter,
    Value,
)
from .types import CEGenerator

Q = TypeVar("Q", bound="Query")
L = TypeVar("L", bound="Lookup")


class Lookup(TypedDict):
    dataset: Optional[Dataset | str]
    schema: Optional[Schema | str]
    prop: Optional[Property | str]
    value: Optional[Value]


class Query:
    filters: set[F] = set()

    def __init__(self, *filters: Iterable[F]):
        self.filters = {f for f in filters}

    def where(self, **lookup: Lookup) -> Q:
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
        if "prop" in lookup and "value" in lookup:
            self.filters.add(
                PropertyFilter(
                    lookup["prop"], lookup["value"], lookup.pop("operator", None)
                )
            )
        return self.__class__(*self.filters)

    def apply(self, proxy: CE) -> bool:
        if not self.filters:
            return True
        return all(f.apply(proxy) for f in self.filters)

    def apply_iter(self, proxies: CEGenerator) -> CEGenerator:
        """
        apply a `Query` to a generator of proxies and return a generator of filtered proxies
        """
        for proxy in proxies:
            if self.apply(proxy):
                yield proxy

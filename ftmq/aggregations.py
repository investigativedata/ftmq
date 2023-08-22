import statistics
from collections import defaultdict
from functools import cache
from typing import Any, Iterable, TypeAlias

from banal import ensure_list
from followthemoney.schema import Schema
from followthemoney.types import registry
from pydantic import BaseModel

from ftmq.enums import Aggregations, Properties
from ftmq.types import CE, CEGenerator
from ftmq.util import to_numeric

Value: TypeAlias = int | float | str


@cache
def get_is_numeric(schema: Schema, prop: str) -> bool:
    prop = schema.get(prop)
    if prop is not None:
        return prop.type == registry.number
    return False


class Aggregation(BaseModel):
    prop: Properties
    func: Aggregations
    values: list[int | float] = []
    value: Value | None = None

    def __hash__(self) -> int:
        return hash((self.prop, self.func))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def get_value(self) -> Value | None:
        if self.func == "min":
            return min(self.values)
        if self.func == "max":
            return max(self.values)
        if self.func == "sum":
            return sum(self.values)
        if self.func == "avg":
            return statistics.mean(self.values)

    def collect(self, proxy: CE) -> CE:
        is_numeric = get_is_numeric(proxy.schema, self.prop)
        for value in proxy.get(self.prop, quiet=True):
            if is_numeric:
                value = to_numeric(value)
            if value is not None:
                self.values.append(value)
        return proxy

    def apply(self, proxies: CEGenerator) -> CEGenerator:
        for proxy in proxies:
            yield self.collect(proxy)
        self.__exit__()

    def __enter__(self) -> "Aggregation":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.value = self.get_value()

    def dict(self, *args, **kwargs) -> dict[str, Any]:
        self.__exit__()
        return super().dict(*args, **kwargs)


AggregatorResult: TypeAlias = dict[Aggregations, dict[Properties, Value]]


class Aggregator(BaseModel):
    aggregations: list[Aggregation] = []
    result: AggregatorResult = defaultdict(dict)

    def __enter__(self) -> "Aggregator":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        for agg in self.aggregations:
            self.result[str(agg.func)][str(agg.prop)] = agg.value

    def apply(self, proxies: CEGenerator) -> CEGenerator:
        for agg in self.aggregations:
            proxies = agg.apply(proxies)
        yield from proxies
        self.__exit__()

    @classmethod
    def from_dict(cls, data: dict[Aggregations, Iterable[Properties]]) -> "Aggregator":
        return cls(
            aggregations=[
                Aggregation(prop=p, func=agg)
                for agg, props in data.items()
                for p in ensure_list(props)
            ]
        )

    def to_dict(self) -> dict[str, set[str]]:
        data = defaultdict(set)
        for agg in self.aggregations:
            data[str(agg.func)].add(str(agg.prop))
        return dict(data)

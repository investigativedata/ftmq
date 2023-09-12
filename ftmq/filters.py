from typing import Any, Iterable, TypeVar

from banal import as_bool, ensure_list, is_listish
from followthemoney import model
from followthemoney.property import Property
from followthemoney.schema import Schema
from followthemoney.types import registry
from nomenklatura.entity import CE

from ftmq.enums import Comparators
from ftmq.exceptions import ValidationError
from ftmq.types import Value


class Lookup:
    IN = Comparators["in"]
    EQUALS = Comparators["eq"]
    NULL = Comparators["null"]

    def __init__(self, comparator: Comparators, value: Value | None = None):
        self.comparator = self.get_comparator(comparator)
        self.value = value

    def __str__(self) -> str:
        return str(self.comparator)

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def get_comparator(self, comparator: str) -> Comparators:
        try:
            return Comparators[comparator]
        except KeyError:
            raise ValidationError(f"Invalid oparator: `{comparator}`")

    def apply(self, value: str | None) -> bool:
        if self.comparator == "eq":
            return value == self.value
        if self.comparator == "not":
            return value != self.value
        if self.comparator == "in":
            return value in self.value
        if self.comparator == "not_in":
            return value not in self.value
        if self.comparator == "startswith":
            return value.startswith(self.value)
        if self.comparator == "endswith":
            return value.endswith(self.value)
        if self.comparator == "null":
            return not value == self.value
        if self.comparator == "gt":
            return value > self.value
        if self.comparator == "gte":
            return value >= self.value
        if self.comparator == "lt":
            return value < self.value
        if self.comparator == "lte":
            return value <= self.value


class BaseFilter:
    key: str
    value: Value
    lookup: Lookup

    def __init__(
        self,
        value: Value,
        comparator: Comparators | None = None,
    ):
        try:
            self.comparator = Comparators[comparator or "eq"]
        except KeyError:
            raise ValidationError(f"Invalid comparator `{comparator}`")
        self.value = self.get_casted_value(value)
        self.lookup = Lookup(self.comparator, self.value)

    def __hash__(self) -> int:
        return hash((self.key, str(self.value)))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def to_dict(self) -> dict[str, Any]:
        if self.comparator == Lookup.EQUALS:
            key = self.key
        else:
            key = f"{self.key}__{self.lookup}"
        return {key: self.value}

    def apply(self, proxy: CE) -> bool:
        return self.lookup.apply(self.value)

    def get_casted_value(self, value: Any) -> Value:
        if self.comparator == Lookup.IN:
            return set([self.stringify(v) for v in ensure_list(value)])
        if self.comparator == Lookup.NULL:
            return as_bool(value)
        if is_listish(value):
            raise ValidationError(f"Invalid value for `{self.comparator}`: {value}")
        return self.stringify(value) if value is not None else None

    def stringify(self, value: Any) -> str:
        if hasattr(value, "name"):
            return value.name
        return str(value)


class DatasetFilter(BaseFilter):
    key = "dataset"

    def apply(self, proxy: CE) -> bool:
        if self.comparator == Lookup.EQUALS:
            return self.value in proxy.datasets
        for value in proxy.datasets:
            if self.lookup.apply(value):
                return True
        return False


class SchemaFilter(BaseFilter):
    key = "schema"

    def __init__(
        self,
        value: Value | Schema | Iterable[Schema],
        comparator: Comparators | None = None,
        include_descendants: bool = False,
        include_matchable: bool = False,
    ):
        super().__init__(value, comparator)
        self.schemata: set[Schema] = set()
        for schema in ensure_list(value):
            schema = model.get(schema)
            if schema is not None:
                self.schemata.add(schema)
                if include_descendants:
                    self.schemata.update(schema.descendants)
                if include_matchable:
                    self.schemata.update(schema.matchable_schemata)
        if len(self.schemata) == 1:
            self.value = [s.name for s in self.schemata][0]
        elif self.comparator == Lookup.EQUALS:
            if model.get(self.value) is None:
                raise ValidationError(f"Invalid schema: `{self.value}`")

    def apply(self, proxy: CE) -> bool:
        if len(self.schemata) > 1:
            return proxy.schema in self.schemata
        return self.lookup.apply(proxy.schema.name)


class PropertyFilter(BaseFilter):
    def __init__(self, prop: Property, value: Value, comparator: str | None = None):
        super().__init__(value, comparator)
        self.key = self.validate(prop)

    def apply(self, proxy: CE) -> bool:
        for value in proxy.get(self.key, quiet=True):
            if self.lookup.apply(value):
                return True
        return False

    def validate(self, prop: str | Property) -> str:
        if isinstance(prop, Property):
            return prop.name
        if isinstance(prop, str):
            for p in model.properties:
                if p.name == prop or p.qname == prop:
                    return prop
        raise ValidationError(f"Invalid prop: `{prop}`")


class ReverseFilter(BaseFilter):
    """
    Filter for entities that point to a given entity (via id)
    """

    key = "reverse"

    def apply(self, proxy: CE) -> bool:
        for prop, value in proxy.itervalues():
            if prop.type == registry.entity:
                if self.lookup.apply(value):
                    return True
        return False


Filter = DatasetFilter | SchemaFilter | PropertyFilter | ReverseFilter
F = TypeVar("F", bound=Filter)

FILTERS = {
    "dataset": DatasetFilter,
    "schema": SchemaFilter,
    "property": PropertyFilter,
    "reverse": ReverseFilter,
}

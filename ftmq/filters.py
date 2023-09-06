from typing import Any, TypeVar

from banal import as_bool, ensure_list, is_listish
from followthemoney import model
from followthemoney.property import Property
from followthemoney.schema import Schema
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CE

from ftmq.enums import Operators, Properties, Schemata, StrEnum
from ftmq.exceptions import ValidationError
from ftmq.types import Value
from ftmq.util import make_dataset


class BaseFilter:
    instance: Dataset | Schema | Property | None = None
    options: StrEnum = None

    def __init__(self, value: str | Dataset | Schema | Property | None):
        self.instance = self.get_instance(value)
        self.validate()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: `{self.instance}`>"

    def __str__(self) -> str:
        return self.instance.name

    def __hash__(self) -> int:
        return hash((self.get_key(), self.get_value()))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def to_dict(self) -> dict[str, Any]:
        return {self.get_key(): self.get_value()}

    def validate(self):
        if self.options is None:
            return
        try:
            self.options[str(self)]
        except (KeyError, AttributeError):
            raise ValidationError(f"Invalid value: `{self.instance}`")

    def apply(self, proxy: CE) -> bool:
        raise NotImplementedError

    def get_instance(self, value: str) -> Dataset | Schema | Property:
        raise NotImplementedError

    def get_key(self) -> str:
        return self.instance.__class__.__name__.lower()

    def get_value(self) -> str:
        return str(self)


class DatasetFilter(BaseFilter):
    instance: Dataset = None

    def apply(self, proxy: CE) -> bool:
        return self.instance.name in proxy.datasets

    def get_instance(self, value: str | Dataset) -> Dataset:
        if isinstance(value, str):
            value = make_dataset(value)
        return value


class SchemaFilter(BaseFilter):
    instance: Schema = None
    options = Schemata

    def __init__(
        self,
        schema: str | Schema,
        include_descendants: bool = False,
        include_matchable: bool = False,
    ):
        super().__init__(schema)
        self.values: set[Schema] = {self.instance}
        self.include_descendants = include_descendants
        self.include_matchable = include_matchable
        if self.include_descendants:
            self.values.update(self.instance.descendants)
        if self.include_matchable:
            self.values.update(self.instance.matchable_schemata)

    def apply(self, proxy: CE) -> bool:
        for schema in self.values:
            if proxy.schema.name == schema.name:
                return True
        return False

    def get_instance(self, value: str | Schema) -> Schema:
        if isinstance(value, str):
            value = model.get(value)
        return value


class Operator:
    def __init__(self, operator: Operators, value: str | None = None):
        self.operator = self.get_operator(operator)
        self.value = value

    def __str__(self) -> str:
        return str(self.operator)

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def get_value(self):
        if self.operator == "in":
            return ensure_list(self.value)
        if self.operator == "null":
            return as_bool(self.value)
        return str(self.value) if self.value is not None else None

    def get_operator(self, operator: str) -> Operators:
        try:
            return Operators[operator]
        except KeyError:
            raise ValidationError(f"Invalid oparator: `{operator}`")

    def apply(self, value: str | None) -> bool:
        parsed_value = self.get_value()
        if self.operator == "not":
            return value != parsed_value
        if self.operator == "in":
            return value in parsed_value
        if self.operator == "not_in":
            return value not in parsed_value
        if self.operator == "startswith":
            return value.startswith(parsed_value)
        if self.operator == "endswith":
            return value.endswith(parsed_value)
        if self.operator == "null":
            return not value == parsed_value
        if self.operator == "gt":
            return value > parsed_value
        if self.operator == "gte":
            return value >= parsed_value
        if self.operator == "lt":
            return value < parsed_value
        if self.operator == "lte":
            return value <= parsed_value


class PropertyFilter(BaseFilter):
    instance: Property = None
    options = Properties
    value: Value = None
    operator: Operator = None

    def __init__(self, prop: Property, value: Value, operator: str | None = None):
        super().__init__(prop)
        self.value = value
        if operator is not None:
            self.operator = Operator(operator, value)
        else:
            self.operator = None

    def __hash__(self) -> int:
        return hash((self.get_key(), str(self.value)))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def apply(self, proxy: CE) -> bool:
        values = proxy.get(self.instance.name, quiet=True)
        if self.operator is not None:
            for value in values:
                if self.operator.apply(value):
                    return True
        else:
            return self.value in values

    def get_instance(self, value: str | Property) -> Property:
        if isinstance(value, Property):
            return value
        if isinstance(value, str):
            for prop in model.properties:
                if prop.name == value or prop.qname == value:
                    return prop
        raise ValidationError(f"Invalid prop: `{value}`")

    def get_key(self) -> str:
        return self.instance.name

    def get_value(self) -> str | list[str] | dict[str, Any]:
        if self.operator is not None:
            return {str(self.operator): self.casted_value}
        return str(self.value)

    @property
    def casted_value(self) -> str | list[str]:
        if self.operator is not None and self.operator == "in":
            return [str(v) for v in ensure_list(self.value)]
        if is_listish(self.value):
            return [str(v) for v in self.value]
        if self.operator == Operators.null:
            return as_bool(self.value)
        return str(self.value)


Filter = DatasetFilter | SchemaFilter | PropertyFilter
F = TypeVar("F", bound=Filter)

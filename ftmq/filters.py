from typing import TypeVar

from banal import as_bool, ensure_list
from followthemoney import model
from followthemoney.property import Property
from followthemoney.schema import Schema
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CE

from .enums import Operators, Properties, Schemata
from .exceptions import ValidationError
from .types import Value
from .util import StrEnum, make_dataset


class BaseFilter:
    instance: Dataset | Schema | Property | None = None
    options: StrEnum = None

    def __init__(self, value: str | Dataset | Schema | Property | None):
        self.instance = self.get_instance(value)
        self.validate()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: `{self.instance}`>"

    def __str__(self) -> str:
        return str(self.instance)

    def validate(self):
        if self.options is None:
            return
        try:
            self.options[self.get_str_value()]
        except (KeyError, AttributeError):
            raise ValidationError(f"{self}: invalid value: `{self.instance}`")

    def apply(self, proxy: CE) -> bool:
        raise NotImplementedError

    def get_instance(self, value: str) -> Dataset | Schema | Property:
        raise NotImplementedError

    def get_str_value(self) -> str:
        return str(self.instance)


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

    def get_str_value(self) -> str:
        return self.instance.name


class Operator:
    def __init__(self, operator: Operators, value: str | None = None):
        self.operator = self.get_operator(operator)
        self.value = value

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
            raise ValidationError(f"{self}: Invalid oparator: `{operator}`")

    def apply(self, value: str | None) -> bool:
        parsed_value = self.get_value()
        if self.operator == "in":
            return value in parsed_value
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

    def apply(self, proxy: CE) -> bool:
        values = proxy.get(self.instance.name, quiet=True)
        if self.operator is not None:
            for value in values:
                if self.operator.apply(value):
                    return True
        else:
            return self.value in values

    def get_instance(self, value: str | Property) -> Property:
        if isinstance(value, str):
            for prop in model.properties:
                if prop.name == value or prop.qname == value:
                    return prop
        return value

    def get_str_value(self):
        return self.instance.name


Filter = DatasetFilter | SchemaFilter | PropertyFilter
F = TypeVar("F", bound=Filter)

from collections.abc import Iterable
from enum import Enum, EnumMeta
from typing import Any

from followthemoney import model
from nomenklatura.dataset.coverage import DataCoverage


class EMeta(EnumMeta):
    def __contains__(self, member: Any) -> bool:
        try:
            self(member)
            return True
        except ValueError:
            return False


def StrEnum(name: str, values: Iterable[Any]) -> Enum:
    # mimic py3.11 enum.StrEnum
    # and fix default enum implementation:
    # https://gist.github.com/simonwoerpel/bdb9959de75e550349961677549624fb
    class _StrEnum(str, Enum, metaclass=EMeta):
        def __str__(self):
            return self.value

        @property
        def value(self) -> Any:
            return super().value

        @property
        def name(self) -> Any:
            return super().name

        @value.setter
        def value(self, value: str):
            self.value = value

        @name.setter
        def name(self, name: str):
            self.name = name

    return _StrEnum(name, {str(v): str(v) for v in values})


Schemata = StrEnum("Schemata", [k for k in model.schemata.keys()])
Properties = StrEnum("Properties", [n for n in {p.name for p in model.properties}])
PropertyTypes = {p.name: p.type for p in model.properties}
PropertyTypes = Enum("PropertyTypes", PropertyTypes.items())
Comparators = StrEnum(
    "Comparators",
    [
        "eq",
        "not",
        "in",
        "not_in",
        "null",
        "gt",
        "gte",
        "lt",
        "lte",
        "like",
        "ilike",
        "notlike",
        "notilike",
        "between",
        "startswith",
        "endswith",
    ],
)
Frequencies = StrEnum("Frequencies", [f for f in DataCoverage.FREQUENCIES])
Aggregations = StrEnum("Aggregations", ("min", "max", "sum", "avg"))

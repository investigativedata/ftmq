from enum import Enum
from typing import Any, Iterable

from nomenklatura.dataset import DataCatalog, Dataset


def StrEnum(name: str, values: Iterable[Any]) -> Enum:
    # mimic py3.11 enum.StrEnum
    class _StrEnum(str, Enum):
        def __str__(self):
            return self.value

    return _StrEnum(name, {str(v): str(v) for v in values})


def make_dataset(name: str) -> Dataset:
    catalog = DataCatalog(
        Dataset, {"datasets": [{"name": name, "title": name.title()}]}
    )
    return catalog.get(name)

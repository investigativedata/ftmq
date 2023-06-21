from enum import Enum
from typing import Any, Generator, Iterable

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


def parse_unknown_cli_filters(
    filters: tuple[str],
) -> Generator[tuple[str, str, str], None, None]:
    filters = (f for f in filters)
    for prop in filters:
        prop = prop.lstrip("-")
        if "=" in prop:  # 'country=de'
            prop, value = prop.split("=")
        else:  # ("country", "de"):
            value = next(filters)

        prop, *op = prop.split("__")
        yield prop, value, op[0] if op else None

from collections.abc import Generator, Iterable
from functools import cache
from typing import Any

import pycountry
from banal import ensure_list
from nomenklatura.dataset import DataCatalog, Dataset, DefaultDataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.statement import Statement

from ftmq.exceptions import ValidationError
from ftmq.types import SGenerator


@cache
def make_dataset(name: str) -> Dataset:
    catalog = DataCatalog(
        Dataset, {"datasets": [{"name": name, "title": name.title()}]}
    )
    return catalog.get(name)


def parse_unknown_filters(
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


def make_proxy(data: dict[str, Any], dataset: str | Dataset | None = None) -> CE:
    datasets = ensure_list(data.pop("datasets", None))
    if dataset is not None:
        if isinstance(dataset, str):
            dataset = dataset = make_dataset(dataset)
        datasets.append(dataset.name)
    elif datasets:
        dataset = datasets[0]
        dataset = make_dataset(dataset)
    else:
        dataset = DefaultDataset
    proxy = CompositeEntity(dataset, data)
    if len(datasets) > 1:
        if proxy.id is None:
            raise ValidationError("Entity has no ID.")
        statements = get_statements(proxy, *datasets)
        return CompositeEntity.from_statements(dataset, statements)
    return proxy


def get_statements(proxy: CE, *datasets: Iterable[str]) -> SGenerator:
    datasets = datasets or ["default"]
    for dataset in datasets:
        yield from Statement.from_entity(proxy, dataset)


@cache
def get_country_name(alpha2: str) -> str:
    try:
        country = pycountry.countries.get(alpha_2=alpha2.lower())
        return country.name
    except (LookupError, AttributeError):
        return alpha2


def to_numeric(value: Any) -> int | float | None:
    try:
        value = str(value).replace(",", "")
        value = float(value)
        if value == int(value):
            return int(value)
        return float(value)
    except ValueError:
        return

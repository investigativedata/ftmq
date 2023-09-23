import re
from collections.abc import Generator, Iterable
from functools import cache
from typing import Any

import pycountry
from banal import ensure_list
from nomenklatura.dataset import DataCatalog, Dataset, DefaultDataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.statement import Statement
from normality import slugify

from ftmq.enums import Comparators
from ftmq.exceptions import ValidationError
from ftmq.types import SGenerator


@cache
def make_dataset(name: str) -> Dataset:
    catalog = DataCatalog(
        Dataset, {"datasets": [{"name": name, "title": name.title()}]}
    )
    return catalog.get(name)


def parse_comparator(key: str) -> tuple[str, Comparators]:
    key, *comparator = key.split("__", 1)
    if comparator:
        comparator = Comparators[comparator[0]]
    else:
        comparator = Comparators["eq"]
    return key, comparator


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
        # FIXME
        for stmt in Statement.from_entity(proxy, dataset):
            stmt = stmt.to_dict()
            stmt["target"] = stmt.get("target") or False
            stmt["external"] = stmt.get("external") or False
            stmt = Statement.from_dict(stmt)
            yield stmt


@cache
def get_country_name(alpha2: str) -> str:
    try:
        country = pycountry.countries.get(alpha_2=alpha2.lower())
        return country.name
    except (LookupError, AttributeError):
        return alpha2


NUMERIC_US = re.compile(r"^-?\d+(?:,\d{3})*(?:\.\d+)?$")
NUMERIC_DE = re.compile(r"^-?\d+(?:\.\d{3})*(?:,\d+)?$")


def to_numeric(value: str) -> float | int | None:
    value = str(value).strip()
    try:
        value = float(value)
        if int(value) == value:
            return int(value)
        return value
    except ValueError:
        if re.match(NUMERIC_US, value):
            return to_numeric(value.replace(",", ""))
        if re.match(NUMERIC_DE, value):
            return to_numeric(value.replace(".", "").replace(",", "."))


def join_slug(
    *parts: str | None,
    prefix: str | None = None,
    sep: str = "-",
    strict: bool = True,
    max_len: int = 255,
) -> str | None:
    sections = [slugify(p, sep=sep) for p in parts]
    if strict and None in sections:
        return None
    texts = [p for p in sections if p is not None]
    if not len(texts):
        return None
    prefix = slugify(prefix, sep=sep)
    if prefix is not None:
        texts = [prefix, *texts]
    return sep.join(texts)[:max_len].strip(sep)

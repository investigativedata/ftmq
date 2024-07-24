import re
from functools import cache, lru_cache
from typing import Any, Generator

import pycountry
from banal import ensure_list
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.util import make_entity_id, sanitize_text
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.statement import Statement
from normality import collapse_spaces, slugify

from ftmq.enums import Comparators
from ftmq.exceptions import ValidationError
from ftmq.types import SGenerator


@cache
def make_dataset(name: str) -> Dataset:
    return Dataset.make({"name": name, "title": name})


@cache
def ensure_dataset(ds: str | Dataset | None = None) -> Dataset | None:
    if not ds:
        return
    if isinstance(ds, str):
        return make_dataset(ds)
    return ds


DefaultDataset = make_dataset("default")


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
        op = op[0] if op else Comparators.eq
        if op == Comparators["in"]:
            value = value.split(",")
        yield prop, value, op


def make_proxy(data: dict[str, Any], dataset: str | Dataset | None = None) -> CE:
    datasets = ensure_list(data.pop("datasets", None))
    if dataset is not None:
        if isinstance(dataset, str):
            dataset = make_dataset(dataset)
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


def get_statements(proxy: CE, *datasets: str) -> SGenerator:
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


@cache
def get_country_code(value: str | None, splitter: str | None = ",") -> str | None:
    if not value:
        return
    code = registry.country.clean_text(value)
    if code:
        return code
    for token in value.split(splitter):
        code = registry.country.clean_text(token)
        if code:
            return code
    return


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
    slug = sep.join(texts)
    if len(slug) <= max_len:
        return slug
    # shorten slug but ensure unique
    ident = make_entity_id(slug)[:8]
    slug = slug[: max_len - 9].strip(sep)
    return f"{slug}-{ident}"


def get_year(value: Any) -> int | None:
    if not value:
        return
    try:
        return int(str(value)[:4])
    except ValueError:
        return


@lru_cache(1024)
def clean_string(value: Any) -> str | None:
    """
    Convert a value to None or a sanitized string without linebreaks
    """
    value = sanitize_text(value)
    if value is None:
        return
    return collapse_spaces(value)


@lru_cache(1024)
def clean_name(value: Any) -> str | None:
    """
    Clean a value and only return it if it is a "name" in the sense of, doesn't
    contain exclusively of special chars
    """
    value = clean_string(value)
    if slugify(value) is None:
        return
    return value


@lru_cache(1024)
def make_fingerprint(value: Any) -> str | None:
    """
    Create a stable but simplified string or None from input that can be used
    to generate ids (to mimic `fingerprints.generate` which is unstable for IDs
    as its algorithm could change)
    """
    value = clean_name(value)
    if value is None:
        return
    return " ".join(sorted(set(slugify(value).split("-"))))


@lru_cache(1024)
def make_string_id(*values: Any) -> str | None:
    return make_entity_id(*map(clean_name, values))


@lru_cache(1024)
def make_fingerprint_id(*values: Any) -> str | None:
    return make_entity_id(*map(make_fingerprint, values))


@cache
def prop_is_numeric(schema: Schema, prop: str) -> bool:
    prop = schema.get(prop)
    if prop is not None:
        return prop.type == registry.number
    return False


def get_proxy_caption_property(proxy: CE) -> dict[str, str]:
    for prop in proxy.schema.caption:
        for value in proxy.get(prop):
            return {prop: value}
    return {}


def get_dehydrated_proxy(proxy: CE) -> CE:
    """
    reduce proxy payload to only include caption property
    """
    return make_proxy(
        {
            "id": proxy.id,
            "schema": proxy.schema.name,
            "properties": get_proxy_caption_property(proxy),
            "datasets": proxy.datasets,
        }
    )


def get_featured_proxy(proxy: CE) -> CE:
    """
    reduce proxy payload to only include featured properties
    """
    featured = get_dehydrated_proxy(proxy)
    for prop in proxy.schema.featured:
        featured.add(prop, proxy.get(prop))
    return featured

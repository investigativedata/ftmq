import contextlib
import logging
import sys
from collections.abc import Iterable
from typing import Any, Literal

import orjson
from banal import ensure_list, is_listish
from fsspec import open
from nomenklatura.dataset import DefaultDataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.statement import Statement
from nomenklatura.util import PathLike

from ftmq.types import CEGenerator, SDict, SGenerator
from ftmq.util import make_dataset

log = logging.getLogger(__name__)


def make_proxy(data: dict[str, Any], dataset: str | None = None) -> CE:
    datasets = ensure_list(data.pop("datasets", None))
    if dataset is not None:
        datasets.append(dataset)
        dataset = make_dataset(dataset)
    elif datasets:
        dataset = datasets[0]
        dataset = make_dataset(dataset)
    else:
        dataset = DefaultDataset
    proxy = CompositeEntity(dataset, data)
    if datasets:
        statements = get_statements(proxy, *datasets)
        return CompositeEntity.from_statements(dataset, statements)
    return proxy


@contextlib.contextmanager
def smart_open(
    uri: str | None = None,
    sys_io: Literal[sys.stdin.buffer, sys.stdout.buffer] | None = sys.stdin,
    *args,
    **kwargs
):
    is_buffer = False
    kwargs["mode"] = kwargs.get("mode", "rb")
    if uri and uri != "-":
        fh = open(uri, *args, **kwargs)
    else:
        fh = sys_io
        is_buffer = True

    try:
        if is_buffer:
            yield fh
        else:
            yield fh.open()
    finally:
        if not is_buffer:
            fh.close()


def smart_read(uri, *args, **kwargs):
    kwargs["mode"] = kwargs.get("mode", "rb")
    with smart_open(uri, sys.stdin.buffer, *args, **kwargs) as fh:
        return fh.read()


def smart_write(uri, content: bytes, *args, **kwargs):
    kwargs["mode"] = kwargs.get("mode", "wb")
    with smart_open(uri, sys.stdout.buffer, *args, **kwargs) as fh:
        fh.write(content)


def smart_read_proxies(
    uri: PathLike | Iterable[PathLike],
    mode: str | None = "rb",
    serialize: bool | None = True,
) -> CEGenerator:
    if is_listish(uri):
        for u in uri:
            yield from smart_read_proxies(u, mode, serialize)
        return

    with smart_open(uri, sys.stdin.buffer, mode=mode) as fh:
        while True:
            line = fh.readline()
            if not line:
                break
            data = orjson.loads(line)
            if serialize:
                data = make_proxy(data)
            yield data


def smart_write_proxies(
    uri: PathLike,
    proxies: Iterable[CE | SDict],
    mode: str | None = "wb",
    serialize: bool | None = False,
) -> int:
    ix = 0
    with smart_open(uri, sys.stdout.buffer, mode=mode) as fh:
        for proxy in proxies:
            ix += 1
            if serialize:
                proxy = proxy.to_dict()
            datasets = set(ensure_list(proxy.get("datasets")))
            datasets.discard("default")
            proxy["datasets"] = list(datasets)
            fh.write(orjson.dumps(proxy, option=orjson.OPT_APPEND_NEWLINE))
    return ix


def apply_datasets(
    proxies: Iterable[CE], *datasets: Iterable[str], replace: bool | None = False
) -> CEGenerator:
    for proxy in proxies:
        if datasets:
            if not replace:
                datasets = proxy.datasets | set(datasets)
            statements = get_statements(proxy, *datasets)
            dataset = make_dataset(list(datasets)[0])
        yield CompositeEntity.from_statements(dataset, statements)


def get_statements(proxy: CE, *datasets: Iterable[str]) -> SGenerator:
    datasets = datasets or ["default"]
    for dataset in datasets:
        yield from Statement.from_entity(proxy, dataset)

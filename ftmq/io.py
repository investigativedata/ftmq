import contextlib
import logging
import re
import sys
from collections.abc import Iterable
from typing import Any, Literal
from urllib.parse import urlparse

import orjson
from banal import is_listish
from fsspec import open
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.util import PathLike

from ftmq.query import Query
from ftmq.store import Store, get_store
from ftmq.types import CEGenerator, SDict
from ftmq.util import get_statements, make_dataset, make_proxy

log = logging.getLogger(__name__)

store_uri_re = re.compile(r"memory|leveldb|.*sql.*|https?\+aleph")


@contextlib.contextmanager
def smart_open(
    uri: str | None = None,
    sys_io: Literal[sys.stdin.buffer, sys.stdout.buffer] | None = sys.stdin,
    *args,
    **kwargs,
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


def _smart_stream(uri, *args, **kwargs) -> Any:
    kwargs["mode"] = kwargs.get("mode", "rb")
    with smart_open(uri, sys.stdin.buffer, *args, **kwargs) as fh:
        while line := fh.readline():
            yield line


def smart_read(uri, *args, **kwargs) -> Any:
    kwargs["mode"] = kwargs.get("mode", "rb")
    stream = kwargs.pop("stream", False)
    if stream:
        return _smart_stream(uri, *args, **kwargs)

    with smart_open(uri, sys.stdin.buffer, *args, **kwargs) as fh:
        return fh.read()


def smart_write(uri, content: bytes | str, *args, **kwargs) -> Any:
    kwargs["mode"] = kwargs.get("mode", "wb")
    with smart_open(uri, sys.stdout.buffer, *args, **kwargs) as fh:
        fh.write(content)


def smart_get_store(uri: PathLike, **kwargs) -> Store | None:
    uri = str(uri)
    parsed = urlparse(uri)
    if store_uri_re.match(parsed.scheme):
        return get_store(uri, **kwargs)


def smart_read_proxies(
    uri: PathLike | Iterable[PathLike],
    mode: str | None = "rb",
    serialize: bool | None = True,
    query: Query | None = None,
    **store_kwargs,
) -> CEGenerator:
    if is_listish(uri):
        for u in uri:
            yield from smart_read_proxies(u, mode, serialize, query)
        return

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        view = store.query()
        yield from view.entities(query)
        return

    with smart_open(uri, sys.stdin.buffer, mode=mode) as fh:

        def _iter():
            while line := fh.readline():
                data = orjson.loads(line)
                if serialize or query:
                    data = make_proxy(data)
                yield data

        q = query or Query()
        yield from q.apply_iter(_iter())


def smart_write_proxies(
    uri: PathLike,
    proxies: Iterable[CE | SDict],
    mode: str | None = "wb",
    serialize: bool | None = False,
    **store_kwargs,
) -> int:
    ix = 0

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        dataset = store_kwargs.get("dataset")
        if dataset is not None:
            proxies = apply_datasets(proxies, dataset, replace=True)
        with store.writer() as bulk:
            for proxy in proxies:
                ix += 1
                bulk.add_entity(proxy)
        return ix

    with smart_open(uri, sys.stdout.buffer, mode=mode) as fh:
        for proxy in proxies:
            ix += 1
            if serialize:
                proxy = proxy.to_dict()
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

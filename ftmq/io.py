import contextlib
import sys
from typing import Any, Iterable, Literal

import orjson
from banal import is_listish
from followthemoney import model
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.util import PathLike
from smart_open import open

from .types import CEGenerator, SDict


def load_proxy(data: dict[str, Any]) -> CE:
    return CompositeEntity.from_dict(model, data)


@contextlib.contextmanager
def smart_open(
    uri: str | None = None,
    sys_io: Literal[sys.stdin.buffer, sys.stdout.buffer] | None = sys.stdin,
    **kwargs
):
    if uri and uri != "-":
        fh = open(uri, **kwargs)
    else:
        fh = sys_io

    try:
        yield fh
    finally:
        if fh not in (sys.stdout.buffer, sys.stdin.buffer):
            fh.close()


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
                data = load_proxy(data)
                data.datasets.discard("default")
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
            fh.write(orjson.dumps(proxy, option=orjson.OPT_APPEND_NEWLINE))
    return ix


def apply_datasets(
    proxies: Iterable[CE], *datasets: Iterable[str], replace: bool | None = False
) -> CEGenerator:
    for proxy in proxies:
        if datasets:
            if replace:
                proxy.datasets = set(datasets)
            else:
                proxy.datasets.update(datasets)
        yield proxy

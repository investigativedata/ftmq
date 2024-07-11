import logging
from collections.abc import Iterable

import orjson
from anystore.io import smart_open, smart_stream
from banal import is_listish
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.util import PathLike

from ftmq.exceptions import ValidationError
from ftmq.query import Query
from ftmq.store import Store, get_store
from ftmq.types import CEGenerator, SDict
from ftmq.util import get_statements, make_dataset, make_proxy

log = logging.getLogger(__name__)

DEFAULT_MODE = "rb"


def smart_get_store(uri: PathLike, **kwargs) -> Store | None:
    try:
        return get_store(uri, **kwargs)
    except NotImplementedError:
        return


def smart_read_proxies(
    uri: PathLike | Iterable[PathLike],
    mode: str | None = DEFAULT_MODE,
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

    lines = smart_stream(uri)
    lines = (orjson.loads(line) for line in lines)
    if serialize or query:
        q = query or Query()
        proxies = (make_proxy(line) for line in lines)
        yield from q.apply_iter(proxies)
    else:
        for line in lines:
            if line.get("id") is None:
                raise ValidationError("Entity has no ID.")
            yield line


def smart_write_proxies(
    uri: PathLike,
    proxies: Iterable[CE | SDict],
    mode: str | None = "wb",
    serialize: bool | None = False,
    **store_kwargs,
) -> int:
    ix = 0
    if proxies is None:  # FIXME how could this happen
        return ix

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        dataset = store_kwargs.get("dataset")
        if dataset is not None:
            proxies = apply_datasets(proxies, dataset, replace=True)
        with store.writer() as bulk:
            for proxy in proxies:
                ix += 1
                bulk.add_entity(proxy)
                if ix % 1_000 == 0:
                    log.info("Writing proxy %d ..." % ix)
        return ix

    with smart_open(uri, mode=mode) as fh:
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

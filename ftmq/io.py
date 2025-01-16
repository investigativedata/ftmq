from typing import Any, Iterable

import orjson
from anystore.io import Uri, smart_open, smart_stream
from banal import is_listish
from followthemoney import model
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.stream import StreamEntity

from ftmq.logging import get_logger
from ftmq.query import Query
from ftmq.store import Store, get_store
from ftmq.types import CEGenerator, Proxy, SEGenerator
from ftmq.util import ensure_proxy, get_statements, make_dataset, make_proxy

log = get_logger(__name__)

DEFAULT_MODE = "rb"


def smart_get_store(uri: Uri, **kwargs) -> Store | None:
    try:
        return get_store(uri, **kwargs)
    except NotImplementedError:
        return


def smart_read_proxies(
    uri: Uri | Iterable[Uri],
    mode: str | None = DEFAULT_MODE,
    query: Query | None = None,
    **store_kwargs: Any,
) -> CEGenerator:
    """
    Stream proxies from an arbitrary source

    Example:
        ```python
        from ftmq import Query
        from ftmq.io import smart_read_proxies

        # remote file-like source
        for proxy in smart_read_proxies("s3://data/entities.ftm.json"):
            print(proxy.schema)

        # multiple files
        for proxy in smart_read_proxies("./1.json", "./2.json"):
            print(proxy.schema)

        # nomenklatura store
        for proxy in smart_read_proxies("redis://localhost", dataset="default"):
            print(proxy.schema)

        # apply a query to sql storage
        q = Query(dataset="my_dataset", schema="Person")
        for proxy in smart_read_proxies("sqlite:///data/ftm.db", query=q):
            print(proxy.schema)
        ```

    Args:
        uri: File-like uri or store uri or multiple uris
        mode: Open mode for file-like sources (default: `rb`)
        query: Filter `Query` object
        **store_kwargs: Pass through configuration to statement store

    Yields:
        A stream of `nomenklatura.entity.CompositeEntity`
    """
    if is_listish(uri):
        for u in uri:
            yield from smart_read_proxies(u, mode, query)
        return

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        view = store.query()
        yield from view.entities(query)
        return

    q = query or Query()
    lines = smart_stream(uri)
    lines = (orjson.loads(line) for line in lines)
    proxies = (make_proxy(line) for line in lines)
    yield from q.apply_iter(proxies)


def smart_stream_proxies(
    uri: Uri | Iterable[Uri], mode: str | None = DEFAULT_MODE
) -> SEGenerator:
    """
    Stream `nomenklatura.stream.StreamEntity` from fs-like uris.

    Args:
        uri: File-like uri or multiple uris
        mode: Open mode for file-like sources (default: `rb`)

    Yields:
        A stream of `nomenklatura.stream.StreamEntity`
    """
    if is_listish(uri):
        for u in uri:
            yield from smart_stream_proxies(u, mode)
        return

    for line in smart_stream(uri, mode):
        data = orjson.loads(line)
        yield StreamEntity.from_dict(model, data)


def smart_write_proxies(
    uri: Uri,
    proxies: Iterable[Proxy],
    mode: str | None = "wb",
    **store_kwargs: Any,
) -> int:
    """
    Write a stream of proxies (or data dicts) to an arbitrary target.

    Example:
        ```python
        from ftmq.io import smart_write_proxies

        proxies = [...]

        # to a remote cloud storage
        smart_write_proxies("s3://data/entities.ftm.json", proxies)

        # to a redis statement store
        smart_write_proxies("redis://localhost", proxies, dataset="my_dataset")
        ```

    Args:
        uri: File-like uri or store uri
        proxies: Iterable of proxy data
        mode: Open mode for file-like targets (default: `wb`)
        **store_kwargs: Pass through configuration to statement store

    Returns:
        Number of written proxies
    """
    ix = 0
    if not proxies:
        return ix

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        proxies = (ensure_proxy(p) for p in proxies)
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
            data = proxy.to_dict()
            fh.write(orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE))
    return ix


def apply_datasets(
    proxies: Iterable[CE], *datasets: Iterable[str], replace: bool | None = False
) -> CEGenerator:
    """
    Apply datasets to a stream of proxies

    Args:
        proxies: Iterable of `nomenklatura.entity.CompositeEntity`
        *datasets: One or more dataset names to apply
        replace: Drop any other existing datasets

    Yields:
        The proxy stream with the datasets applied
    """
    for proxy in proxies:
        if datasets:
            if not replace:
                datasets = proxy.datasets | set(datasets)
            statements = get_statements(proxy, *datasets)
            dataset = make_dataset(list(datasets)[0])
        yield CompositeEntity.from_statements(dataset, statements)

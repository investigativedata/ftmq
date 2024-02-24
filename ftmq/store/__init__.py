from functools import cache
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlparse

from nomenklatura import Resolver
from nomenklatura.db import get_metadata

from ftmq.dedupe import get_resolver
from ftmq.model.dataset import Catalog, Dataset
from ftmq.store.aleph import AlephStore
from ftmq.store.base import Store, View
from ftmq.store.memory import MemoryStore
from ftmq.store.sql import SQLStore
from ftmq.types import PathLike

S = TypeVar("S", bound=Store)


@cache
def get_store(
    uri: PathLike | None = "memory:///",
    catalog: Catalog | None = None,
    dataset: Dataset | str | None = None,
    resolver: Resolver | str | None = None,
) -> Store:
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    if isinstance(resolver, (str, Path)):
        resolver = get_resolver(resolver)
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(catalog, dataset, resolver=resolver)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        try:
            from ftmq.store.level import LevelDBStore

            return LevelDBStore(catalog, dataset, path=path, resolver=resolver)
        except ImportError:
            raise ImportError("Can not load LevelDBStore. Install `plyvel`")
    if parsed.scheme == "redis":
        try:
            from ftmq.store.redis import RedisStore

            return RedisStore(catalog, dataset, path=path, resolver=resolver)
        except ImportError:
            raise ImportError("Can not load RedisStore. Install `redis`")
    if "sql" in parsed.scheme:
        get_metadata.cache_clear()
        return SQLStore(catalog, dataset, uri=uri, resolver=resolver)
    if "aleph" in parsed.scheme:
        return AlephStore.from_uri(
            uri, catalog=catalog, dataset=dataset, resolver=resolver
        )
    raise NotImplementedError(uri)


__all__ = ["get_store", "S", "Store", "View", "MemoryStore", "SQLStore", "AlephStore"]

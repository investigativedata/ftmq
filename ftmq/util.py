from io import BytesIO
from typing import Any, Generator

import orjson
from followthemoney import model
from nomenklatura.dataset import DataCatalog, Dataset
from nomenklatura.entity import CE, CompositeEntity

Proxies = Generator[CE, None, None]
Value = list[str]


def get_dataset(name: str) -> Dataset:
    catalog = DataCatalog(
        Dataset, {"datasets": [{"name": name, "title": name.title()}]}
    )
    return catalog.get(name)


def get_proxy(data: dict[str, Any]) -> CE:
    return CompositeEntity.from_dict(model, data)


def read_proxies(i: BytesIO) -> Proxies:
    while True:
        line = i.readline()
        if not line:
            return

        proxy = get_proxy(orjson.loads(line))
        yield proxy


def write_proxy(o: BytesIO, proxy: CE):
    o.write(orjson.dumps(proxy.to_dict(), option=orjson.OPT_APPEND_NEWLINE))

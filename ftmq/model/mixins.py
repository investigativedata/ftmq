import logging
from functools import cache
from pathlib import Path
from typing import Any

import orjson
import yaml
from nomenklatura.dataset.catalog import DataCatalog as NKCatalog
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.dataset import Dataset as NKDataset
from nomenklatura.dataset.publisher import DataPublisher as NKPublisher
from nomenklatura.dataset.resource import DataResource as NKResource
from pydantic import BaseModel as _BaseModel

from ftmq.types import PathLike

log = logging.getLogger(__name__)


@cache
def cached_from_uri(uri: str) -> dict[str, Any]:
    """
    Cache RemoteMixin on runtime
    """
    from ftmq.io import smart_read

    log.info("Loading `%s` ..." % uri)
    data = smart_read(uri)
    return orjson.loads(data)


class RemoteMixin:
    """
    Load a pydantic model from a remote uri, such as dataset index.json or catalog.json
    """

    def __init__(self, **data):
        """
        Update with remote data, but local data takes precedence
        """
        if data.get("uri"):
            remote = self.from_uri(data["uri"])
            data = {**remote.dict(), **data}
        super().__init__(**data)

    @classmethod
    def from_uri(cls, uri: str) -> "RemoteMixin":
        data = cached_from_uri(uri)
        return cls(**data)


class YamlMixin:
    """
    Load a pydantic model from yaml spec
    """

    @classmethod
    def from_string(cls, data: str, **kwargs) -> "YamlMixin":
        data = yaml.safe_load(data)
        return cls(**data, **kwargs)

    @classmethod
    def from_path(cls, fp: PathLike) -> "YamlMixin":
        from ftmq.io import smart_read

        data = smart_read(fp)
        return cls.from_string(data, base_path=Path(fp).parent)


class BaseModel(_BaseModel):
    def __hash__(self) -> int:
        return hash(repr(self.dict()))


class NKModel(RemoteMixin, YamlMixin, BaseModel):
    class Config:
        json_encoders = {
            NKCatalog: lambda x: x.to_dict(),
            NKDataset: lambda x: x.to_dict(),
            NKPublisher: lambda x: x.to_dict(),
            NKCoverage: lambda x: x.to_dict(),
            NKResource: lambda x: x.to_dict(),
        }

    def to_nk(self) -> NKCatalog | NKCoverage | NKDataset | NKPublisher | NKResource:
        return self._nk_model(self.dict())

    def __getattr__(self, attr: str, default: Any | None = None) -> Any:
        return getattr(self.to_nk(), attr, default)

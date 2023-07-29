import os
from pathlib import Path
from typing import Any, Generator, Literal, TypeAlias

from nomenklatura.entity import CE

from .enums import Frequencies, Properties, Schemata

# a string-keyed dict
SDict: TypeAlias = dict[str, Any]

# property multi-value
Value: TypeAlias = list[str]

# composite entity generator
CEGenerator: TypeAlias = Generator[CE, None, None]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]

Schemata: TypeAlias = Literal[tuple(s.name for s in Schemata)]
Properties: TypeAlias = Literal[tuple(p.name for p in Properties)]
Frequencies: TypeAlias = Literal[tuple(f.name for f in Frequencies)]

PathLike: TypeAlias = str | os.PathLike[str] | Path


__all__ = [
    BytesGenerator,
    CE,
    CEGenerator,
    Frequencies,
    PathLike,
    Properties,
    Schemata,
    SDict,
    StrGenerator,
    Value,
]

import os
from collections.abc import Generator
from pathlib import Path
from typing import Any, Literal, TypeAlias

from nomenklatura.entity import CE
from nomenklatura.statement.statement import S

from ftmq import enums

# a string-keyed dict
SDict: TypeAlias = dict[str, Any]

# property multi-value
Value: TypeAlias = list[str]

# composite entity generator
CEGenerator: TypeAlias = Generator[CE, None, None]
SGenerator: TypeAlias = Generator[S, None, None]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]

Schemata: TypeAlias = Literal[tuple(s.name for s in enums.Schemata)]
Properties: TypeAlias = Literal[tuple(p.name for p in enums.Properties)]
Frequencies: TypeAlias = Literal[tuple(f.name for f in enums.Frequencies)]

PathLike: TypeAlias = str | os.PathLike[str] | Path

__all__ = [
    "BytesGenerator",
    "CE",
    "CEGenerator",
    "Frequencies",
    "PathLike",
    "Properties",
    "Schemata",
    "SDict",
    "SGenerator",
    "StrGenerator",
    "Value",
]

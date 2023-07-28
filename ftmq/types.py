from typing import Any, Generator, Literal, TypeAlias

from nomenklatura.entity import CE

from .enums import Properties, Schemata

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

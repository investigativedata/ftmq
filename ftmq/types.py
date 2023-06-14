from typing import Any, Generator, TypeAlias

from nomenklatura.entity import CE

# a string-keyed dict
SDict: TypeAlias = dict[str, Any]

# property multi-value
Value: TypeAlias = list[str]

# composite entity generator
CEGenerator: TypeAlias = Generator[CE, None, None]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]

import logging
from functools import cache

from anystore import smart_stream
from nomenklatura.entity import CompositeEntity
from nomenklatura.resolver import Edge, Resolver

log = logging.getLogger(__name__)


@cache
def get_resolver(uri: str | None = None) -> Resolver[CompositeEntity]:
    resolver = Resolver()
    if not uri:
        return resolver
    for ix, edge in enumerate(smart_stream(uri)):
        edge = Edge.from_line(edge)
        resolver._register(edge)
        if ix and ix % 10_000 == 0:
            log.info("Loading edge %d ..." % ix)
    return resolver

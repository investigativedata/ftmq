import logging
from functools import cache

from nomenklatura.entity import CompositeEntity
from nomenklatura.resolver import Edge, Resolver

log = logging.getLogger(__name__)


@cache
def get_resolver(uri: str | None = None) -> Resolver[CompositeEntity]:
    from ftmq.io import smart_read

    resolver = Resolver()
    if not uri:
        return resolver
    for ix, edge in enumerate(smart_read(uri, stream=True)):
        edge = Edge.from_line(edge)
        resolver._register(edge)
        if ix and ix % 10_000 == 0:
            log.info("Loading edge %d ..." % ix)
    return resolver

from collections import defaultdict
from collections.abc import Generator
from functools import cached_property
from typing import Any
from urllib.parse import urlparse, urlunparse

from alephclient.api import AlephAPI
from alephclient.settings import API_KEY, HOST, MAX_TRIES
from followthemoney.namespace import Namespace
from nomenklatura.dataset import DS
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement
from nomenklatura.store import Store, View, Writer

from ftmq.util import make_proxy

uns = Namespace()


def parse_uri(uri: str) -> tuple[str, str | None, str | None]:
    """
    http+aleph://host.org
    http+aleph://dataset@host.org
    https+aleph://dataset:api_key@host.org
    """
    api_key = API_KEY
    dataset = None
    parsed = urlparse(uri)
    scheme = parsed.scheme.split("+")[0]
    *datasets, host = parsed.netloc.split("@", 1)
    host = urlunparse([scheme, host, *parsed[2:]])
    if len(datasets) == 1:
        dataset, *api_key = datasets[0].split(":", 1)
        if len(api_key) == 1:
            api_key = api_key[0]
    return host, api_key or None, dataset


class AlephStore(Store[CE, DS]):
    def __init__(
        self,
        dataset: DS,
        linker: Resolver,
        host: str | None = None,
        api_key: str | None = None,
    ):
        super().__init__(dataset, linker)
        self.host = host or HOST
        self.api_key = api_key or API_KEY

    @cached_property
    def api(self):
        return AlephAPI(self.host, self.api_key, retries=MAX_TRIES)

    @cached_property
    def collection(self) -> dict[str, Any]:
        return self.api.load_collection_by_foreign_id(self.dataset.name)

    def view(self, scope: DS, external: bool = False) -> View[DS, CE]:
        return AlephView(self, scope, external=external)

    def writer(self) -> Writer[DS, CE]:
        return AlephWriter(self)


class AlephView(View[CE, DS]):
    def __init__(
        self, store: AlephStore[DS, CE], scope: DS, external: bool = False
    ) -> None:
        super().__init__(store, scope, external=external)
        self.store: AlephStore[DS, CE] = store

    def entities(self, *args) -> Generator[CE, None, None]:
        for proxy in self.store.api.stream_entities(self.store.collection):
            proxy = make_proxy(proxy, dataset=self.store.dataset.name)
            yield uns.apply(proxy)

    def get_entity(self, id: str) -> CE | None:
        ns = Namespace(self.store.dataset.name)
        entity_id = ns.sign(id)
        proxy = self.store.api.get_entity(entity_id)
        if proxy is not None:
            proxy = make_proxy(proxy, self.store.dataset.name)
            return uns.apply(proxy)
        return None


class AlephWriter(Writer[DS, CE]):
    BATCH = 1_000

    def __init__(self, store: AlephStore[DS, CE]):
        self.store: AlephStore[DS, CE] = store
        self.batch: dict[str, set[Statement]] = defaultdict(set)

    def flush(self) -> None:
        entities = []
        if self.batch:
            for stmts in self.batch.values():
                entities.append(
                    CompositeEntity.from_statements(self.store.dataset, stmts)
                )
            self.store.api.write_entities(self.store.collection.get("id"), entities)
        self.batch = defaultdict(set)

    def add_statement(self, stmt: Statement) -> None:
        if stmt.entity_id is None:
            return
        if len(self.batch) >= self.BATCH:
            self.flush()
        canonical_id = self.store.linker.get_canonical(stmt.entity_id)
        stmt.canonical_id = canonical_id
        self.batch[stmt.canonical_id].add(stmt)

    def pop(self, entity_id: str) -> list[Statement]:
        # FIXME this actually doesn't delete anything
        self.flush()
        statements: list[Statement] = []
        view = self.store.default_view()
        entity = view.get_entity(entity_id)
        if entity is not None:
            statements = list(entity.statements)
        return statements

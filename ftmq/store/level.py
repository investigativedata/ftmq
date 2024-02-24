from nomenklatura.dataset import DS
from nomenklatura.store import level

from ftmq.model import Catalog
from ftmq.store.base import Store, View


class LevelDBQueryView(View, level.LevelDBView):
    pass


class LevelDBStore(Store, level.LevelDBStore):
    def get_catalog(self) -> Catalog:
        names: set[str] = set()
        with self.db.iterator(prefix=b"e:", include_value=False) as it:
            for k in it:
                _, _, dataset = k.decode("utf-8").split(":", 2)
                names.add(dataset)
        return Catalog.from_names(names)

    def query(
        self, scope: DS | None = None, external: bool = False
    ) -> LevelDBQueryView:
        scope = scope or self.dataset
        return LevelDBQueryView(self, scope, external=external)

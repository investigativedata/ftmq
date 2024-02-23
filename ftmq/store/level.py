from nomenklatura import store as nk
from nomenklatura.store import level

from ftmq.model import Catalog
from ftmq.store import CE, DS, Store, View


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

    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return LevelDBQueryView(self, scope, external=external)

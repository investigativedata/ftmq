from nomenklatura import store as nk
from nomenklatura.dataset import DS

from ftmq.model.dataset import Catalog
from ftmq.store.base import Store, View


class MemoryQueryView(View, nk.memory.MemoryView):
    pass


class MemoryStore(Store, nk.SimpleMemoryStore):
    def get_catalog(self) -> Catalog:
        return Catalog.from_names(self.entities.keys())

    def query(self, scope: DS | None = None, external: bool = False) -> MemoryQueryView:
        scope = scope or self.dataset
        return MemoryQueryView(self, scope, external=external)

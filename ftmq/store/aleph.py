from typing import Self

from nomenklatura.dataset import DS
from nomenklatura.resolver import Resolver

from ftmq.aleph import AlephStore as _AlephStore
from ftmq.aleph import AlephView, parse_uri
from ftmq.model.dataset import Catalog, Dataset
from ftmq.store.base import Store, View
from ftmq.util import DefaultDataset


class AlephQueryView(View, AlephView):
    pass


class AlephStore(Store, _AlephStore):
    def get_catalog(self) -> Catalog:
        # FIXME
        # api.filter_collections("*")
        return Catalog.from_names(DefaultDataset.leaf_names)

    def query(self, scope: DS | None = None, external: bool = False) -> AlephQueryView:
        scope = scope or self.dataset
        return AlephQueryView(self, scope, external=external)

    @classmethod
    def from_uri(
        cls,
        uri: str,
        dataset: Dataset | str | None = None,
        catalog: Catalog | None = None,
        linker: Resolver | None = None,
    ) -> Self:
        host, api_key, foreign_id = parse_uri(uri)
        if dataset is None and foreign_id is not None:
            dataset = foreign_id
        if dataset is not None:
            if isinstance(dataset, str):
                dataset = Dataset(name=dataset)

        return cls(catalog, dataset, linker=linker, host=host, api_key=api_key)

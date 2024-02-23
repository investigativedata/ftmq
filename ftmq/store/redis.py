from nomenklatura import store as nk
from nomenklatura.store import redis

# from ftmq.model import Catalog
from ftmq.store import CE, DS, Store, View


class RedisQueryView(View, redis.RedisView):
    pass


class RedisStore(Store, redis.RedisStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return RedisQueryView(self, scope, external=external)

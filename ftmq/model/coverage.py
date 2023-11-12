from collections import Counter
from typing import Any

from banal import ensure_list
from followthemoney import model
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage

from ftmq.enums import Properties
from ftmq.model.mixins import BaseModel, NKModel
from ftmq.types import CE, CEGenerator, DateLike, Frequencies
from ftmq.util import get_country_name


class Schema(BaseModel):
    name: str
    count: int
    label: str
    plural: str

    def __init__(self, **data):
        schema = model.get(data["name"])
        data["label"] = schema.label
        data["plural"] = schema.plural
        super().__init__(**data)


class Country(BaseModel):
    code: str
    count: int
    label: str | None = None

    def __init__(self, **data):
        data["label"] = get_country_name(data["code"])
        super().__init__(**data)


class Collector:
    def __init__(self):
        self.schemata = Counter()
        self.countries = Counter()
        self.start = set()
        self.end = set()

    def collect(self, proxy: CE) -> None:
        self.schemata[proxy.schema.name] += 1
        for country in proxy.countries:
            self.countries[country] += 1
        self.start.update(proxy.get(Properties.startDate, quiet=True))
        self.start.update(proxy.get(Properties.date, quiet=True))
        self.end.update(proxy.get(Properties.endDate, quiet=True))
        self.end.update(proxy.get(Properties.date, quiet=True))

    def export(self) -> "Coverage":
        start = min(self.start) if self.start else None
        end = max(self.end) if self.end else None
        return Coverage(
            start=start,
            end=end,
            schemata=[Schema(name=k, count=v) for k, v in self.schemata.items()],
            countries=[Country(code=k, count=v) for k, v in self.countries.items()],
            entities=self.schemata.total(),
            years=(
                start[:4] if start else None,
                end[:4] if end else None,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.export()
        return data.model_dump()

    def apply(self, proxies: CEGenerator) -> CEGenerator:
        """
        Generate coverage from an input stream of proxies
        This returns a generator again, so actual collection of coverage stats
        will happen if the actual generator is executed
        """
        for proxy in proxies:
            self.collect(proxy)
            yield proxy


class Coverage(NKModel):
    _nk_model = NKCoverage

    start: DateLike | None = None
    end: DateLike | None = None
    frequency: Frequencies | None = "unknown"

    # own additions:
    schemata: list[Schema] | None = []
    countries: list[Country] | None = []
    entities: int = 0
    years: tuple[int | None, int | None] | None = (None, None)

    def __init__(self, **data):
        if len(ensure_list(data.get("years"))) != 2:
            data["years"] = None
        super().__init__(**data)

    def apply(self, proxies: CEGenerator) -> "Coverage":
        """
        Generate coverage from an input stream of proxies
        """
        if self._collector is None:
            self._collector = Collector()
        for proxy in proxies:
            self._collector.collect(proxy)
        self.__exit__()

    def to_nk(self) -> NKCoverage:
        data = self.model_dump()
        data["countries"] = [c["code"] for c in data["countries"]]
        return NKCoverage(data)

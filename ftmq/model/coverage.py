from collections import Counter
from typing import Any

from followthemoney import model

from ftmq.enums import Properties
from ftmq.model.mixins import BaseModel
from ftmq.types import CE, CEGenerator, Frequencies
from ftmq.util import get_country_name, get_year_from_iso


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


class Schemata(BaseModel):
    total: int = 0
    countries: list[Country] | None = []
    schemata: list[Schema] | None = []


class Coverage(BaseModel):
    start: str | None = None
    end: str | None = None
    frequency: Frequencies | None = "unknown"
    countries: list[str] | None = []
    schedule: str | None = None

    @property
    def years(self) -> tuple[int | None, int | None]:
        """
        Return min / max year extend
        """
        return get_year_from_iso(self.start), get_year_from_iso(self.end)


class DatasetStats(BaseModel):
    coverage: Coverage
    things: Schemata
    intervals: Schemata
    entity_count: int | None = 0


class Collector:
    def __init__(self):
        self.things = Counter()
        self.things_countries = Counter()
        self.intervals = Counter()
        self.intervals_countries = Counter()
        self.start = set()
        self.end = set()

    def collect(self, proxy: CE) -> None:
        if proxy.schema.is_a("Thing"):
            self.things[proxy.schema.name] += 1
            for country in proxy.countries:
                self.things_countries[country] += 1
        else:
            self.intervals[proxy.schema.name] += 1
            for country in proxy.countries:
                self.intervals_countries[country] += 1
        self.start.update(proxy.get(Properties.startDate, quiet=True))
        self.start.update(proxy.get(Properties.date, quiet=True))
        self.end.update(proxy.get(Properties.endDate, quiet=True))
        self.end.update(proxy.get(Properties.date, quiet=True))

    def export(self) -> DatasetStats:
        start = min(self.start) if self.start else None
        end = max(self.end) if self.end else None
        countries = set(self.things_countries.keys()) | set(
            self.intervals_countries.keys()
        )
        coverage = Coverage(start=start, end=end, countries=countries)
        things = Schemata(
            schemata=[Schema(name=k, count=v) for k, v in self.things.items()],
            countries=[
                Country(code=k, count=v) for k, v in self.things_countries.items()
            ],
            total=self.things.total(),
        )
        intervals = Schemata(
            schemata=[Schema(name=k, count=v) for k, v in self.intervals.items()],
            countries=[
                Country(code=k, count=v) for k, v in self.intervals_countries.items()
            ],
            total=self.intervals.total(),
        )
        return DatasetStats(
            coverage=coverage,
            things=things,
            intervals=intervals,
            entity_count=things.total + intervals.total,
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

    def collect_many(self, proxies: CEGenerator) -> DatasetStats:
        for proxy in proxies:
            self.collect(proxy)
        return self.export()

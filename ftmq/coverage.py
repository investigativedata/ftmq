from collections import Counter
from datetime import date
from typing import Any

from banal import ensure_dict
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.util import cleanup

from .enums import Properties
from .types import CE, Frequencies, Schemata


class Collector:
    schemata: Counter = None
    countries: set[str] = None
    start: set[date] = None
    end: set[date] = None

    def __init__(self):
        self.schemata = Counter()
        self.countries = set()
        self.start = set()
        self.end = set()

    def collect(self, proxy: CE) -> None:
        self.schemata[proxy.schema.name] += 1
        self.countries.update(proxy.countries)
        self.start.update(proxy.get(Properties.startDate, quiet=True))
        self.start.update(proxy.get(Properties.date, quiet=True))
        self.end.update(proxy.get(Properties.endDate, quiet=True))
        self.end.update(proxy.get(Properties.date, quiet=True))

    def close(self) -> "Coverage":
        return Coverage(
            {
                "start": min(self.start),
                "end": max(self.end),
                "schemata": dict(self.schemata),
                "countries": self.countries,
                "entities": self.schemata.total(),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.close()
        return data.to_dict()


class Coverage(NKCoverage):
    start: date | None = None
    end: date | None = None
    countries: list[str] | None = None
    frequency: Frequencies | None = None
    schemata: dict[Schemata, int] | None = None
    entities: int | None = 0

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        data = ensure_dict(data)
        super().__init__(data)
        self.schemata = data.get("schemata", {})
        self.entities = data.get("entities", 0)

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["entities"] = self.entities
        data["schemata"] = self.schemata
        return cleanup(data)

    def __enter__(self):
        self._collector = Collector()
        return self._collector

    def __exit__(self, *args, **kwargs):
        res = self._collector.close()
        self._collector = None
        self.start = res.start
        self.end = res.end
        self.schemata = res.schemata
        self.entities = res.entities
        self.countries = res.countries

from followthemoney import model

from .util import StrEnum

Schemata = StrEnum("Schemata", [k for k in model.schemata.keys()])
Properties = StrEnum("Properties", [n for n in {p.name for p in model.properties}])
Operators = StrEnum("Operators", ["in", "null", "gt", "gte", "lt", "lte"])

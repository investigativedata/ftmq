[![ftmq on pypi](https://img.shields.io/pypi/v/ftmq)](https://pypi.org/project/ftmq/) [![Python test and package](https://github.com/investigativedata/ftmq/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/ftmq/actions/workflows/python.yml) [![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit) [![Coverage Status](https://coveralls.io/repos/github/investigativedata/ftmq/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/ftmq?branch=main) [![MIT License](https://img.shields.io/pypi/l/ftmq)](./LICENSE)

# ftmq

An attempt towards a followthemoney query dsl.

This library provides methods to query and filter entities formatted as
[followthemoney](https://github.com/alephdata/followthemoney) data, either from
a json file/stream or using a SQL backend via
[followthemoney-store](https://github.com/alephdata/followthemoney-store)

It also provides a `Query` class that can be used in other libs to work with
SQL queries or api queries.

**Minimum Python version: 3.10**

## Installation

    pip install ftmq

## Usage

`ftmq` accepts either a line-based input stream or an argument with a file uri.
(For integration with `followthemoney-store`, see below)

Input stream:

    cat entities.ftm.json | ftmq <filter expression> > output.ftm.json

URI argument:

Under the hood, `ftmq` uses
[smart_open](https://github.com/RaRe-Technologies/smart_open) to be able to
interpret arbitrary file uris as argument `-i`:

    ftmq <filter expression> -i ~/Data/entities.ftm.json
    ftmq <filter expression> -i https://example.org/data.json.gz
    ftmq <filter expression> -i s3://data-bucket/entities.ftm.json
    ftmq <filter expression> -i webhdfs://host:port/path/file

[...and so on](https://github.com/RaRe-Technologies/smart_open#how)

Of course, the same is possible for output `-o`:

    cat data.json | ftmq <filter expression> -o s3://data-bucket/output.json

### Filter for a dataset:

    cat entities.ftm.json | ftmq -d ec_meetings

### Filter for a schema:

    cat entities.ftm.json | ftmq -s Person

Filter for a schema and all it's descendants or ancestors:

    cat entities.ftm.json | ftmq -s LegalEntity --schema-include-descendants
    cat entities.ftm.json | ftmq -s LegalEntity --schema-include-ancestors

### Filter for properties:

[Properties](https://followthemoney.tech/explorer/) are options via `--<prop>=<value>`

    cat entities.ftm.json | ftmq -s Company --country=de

#### Comparison lookups for properties:

    cat entities.ftm.json | ftmq -s Company --incorporationDate__gte=2020 --address__ilike=berlin

Possible lookups:
- `gt` - greater than
- `lt` - lower than
- `gte` - greater or equal
- `lte` - lower or equal
- `like` - SQLish `LIKE` (use `%` placeholders)
- `ilike` - SQLish `ILIKE`, case-insensitive (use `%` placeholders)
- `[]` - usage: `prop[]=foo` evaluates if `foo` is member of array `prop`


### ftmq apply

"Uplevel" an entity input stream to `nomenklatura.entity.CompositeEntity` and
optionally apply a dataset.

    ftmq apply -i ./entities.ftm.json -d <aditional_dataset>

Overwrite datasets:

    ftmq apply -i ./entities.ftm.json -d <aditional_dataset> --replace-dataset

### ftmstore (database read)

**NOT IMPLEMENTED YET**

The same cli logic applies:

    ftmq store iterate -d ec_meetings -s Event --date__gte=2019 --date__lte=2020

## Python Library

**NOT IMPLEMENTED YET**

```python
from ftmq import Query

q = Query(engine="sqlite") \
    .where(dataset="ec_meetings", date__lte=2020) \
    .where(schema="Event") \
    .order_by("date", ascending=False)

# resulting sqlite query:
str(q)
"""
SELECT t.id,
    t.schema,
    t.entity,
    json_extract(t.entity, '$.properties.date') AS date
FROM ec_meetings t
WHERE
    (EXISTS (SELECT 1 FROM json_each(date) WHERE value <= ?)) AND (t.schema = ?)
ORDER BY date DESC
"""

# parameterized
[p for p in q.parameters]
[2020, 'Event']
```

## support

*This project is part of [investigraph](https://github.com/investigativedata/investigraph)*

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>

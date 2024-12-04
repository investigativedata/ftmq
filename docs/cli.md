`ftmq` accepts either a line-based input stream an argument with a file uri or a store uri to read (or write) [Follow The Money Entities](https://followthemoney.tech/docs/).

Input stream:

```bash
cat entities.ftm.json | ftmq <filter expression> > output.ftm.json
```

Under the hood, `ftmq` uses [anystore](https://github.com/investigativedata/anystore) to be able to interpret arbitrary file uris as argument `-i`:

```bash
ftmq <filter expression> -i ~/Data/entities.ftm.json
ftmq <filter expression> -i https://example.org/data.json.gz
ftmq <filter expression> -i s3://data-bucket/entities.ftm.json
ftmq <filter expression> -i webhdfs://host:port/path/file
```

Of course, the same is possible for output `-o`:

    cat data.json | ftmq <filter expression> -o s3://data-bucket/output.json

## Filter expressions

Filter for a dataset:

```bash
cat entities.ftm.json | ftmq -d ec_meetings
```

Filter for a schema:

```bash
cat entities.ftm.json | ftmq -s Person
```

Filter for a schema and all it's descendants or ancestors:

```bash
cat entities.ftm.json | ftmq -s LegalEntity --schema-include-descendants
cat entities.ftm.json | ftmq -s LegalEntity --schema-include-ancestors
```

Filter for properties:

[Properties](https://followthemoney.tech/explorer/) are cli options via `--<prop>=<value>`

```bash
cat entities.ftm.json | ftmq -s Company --country=de
```

### Comparison lookups for properties

```bash
cat entities.ftm.json | ftmq -s Company --incorporationDate__gte=2020 --address__ilike=berlin
```

Possible lookups:

- `gt` - greater than
- `lt` - lower than
- `gte` - greater or equal
- `lte` - lower or equal
- `like` - SQLish `LIKE` (use `%` placeholders)
- `ilike` - SQLish `ILIKE`, case-insensitive (use `%` placeholders)
- `not` - negative lookup

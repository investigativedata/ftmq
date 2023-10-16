# ftmq

## CHANGELOG

### 0.3.4 (2023-10-16)

- Add group based aggregation, aggregate by year
- Add some util functions from downstream dependencies
- Update dependencies

### 0.3.3 (2023-10-03)

- Update dependencies
- add `get_country_code` util function

### 0.3.2 (2023-09-28)

- Implement simple search
- Add id filters

### 0.3.1 (2023-09-19)

- Refactor filters to allow comparator lookups for `schema` and `dataset` as well

### 0.3.0 (2023-09-06)

- Add `nomeklatura` based stores and query views built upon that (needs documentation)

### 0.2.1 (2023-08-07)

- Align with `nomenklatura` v3

### 0.2.0 (2023-07-31)

- Model [nomenklatura](https://github.com/opensanctions/nomenklatura) catalog/dataset via pydantic
- Replace `smart_open` with [fsspec](https://github.com/fsspec/filesystem_spec)
- add generic IO handling based on `fsspec`

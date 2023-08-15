import pytest
from sqlalchemy.sql.selectable import Select

from ftmq.exceptions import ValidationError
from ftmq.query import Query


def _compare_str(s1, s2) -> bool:
    return " ".join(str(s1).split()).strip() == " ".join(str(s2).split()).strip()


def test_sql():
    q = (
        Query()
        .where(dataset="test")
        .where(dataset="other", schema="Event")
        .where(prop="date", value=2023, operator="gte")
    )
    whereclause = """WHERE (nk_store.dataset = :dataset_1 OR nk_store.dataset = :dataset_2)
    AND nk_store.schema = :schema_1
    AND nk_store.prop = :prop_1
    AND nk_store.value >= :value_1"""
    fields = """nk_store.id, nk_store.entity_id, nk_store.canonical_id, nk_store.prop,
    nk_store.prop_type, nk_store.schema, nk_store.value, nk_store.original_value,
    nk_store.dataset, nk_store.lang, nk_store.target, nk_store.external,
    nk_store.first_seen, nk_store.last_seen"""
    assert isinstance(q.sql.entity_ids, Select)
    assert _compare_str(
        q.sql.entity_ids,
        f"""
        SELECT nk_store.entity_id
        FROM nk_store {whereclause}
        """,
    )

    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM nk_store
        WHERE nk_store.entity_id IN (SELECT nk_store.entity_id FROM nk_store {whereclause})
        ORDER BY nk_store.entity_id
        """,
    )

    assert isinstance(q.sql.count, Select)
    assert _compare_str(
        q.sql.count,
        f"""
        SELECT count(DISTINCT nk_store.entity_id) AS count_1
        FROM nk_store {whereclause}
        """,
    )

    assert isinstance(q.sql.datasets, Select)
    assert _compare_str(
        q.sql.datasets,
        f"""
        SELECT nk_store.dataset, count(DISTINCT nk_store.entity_id) AS count_1
        FROM nk_store {whereclause}
        GROUP BY nk_store.dataset
        """,
    )

    assert isinstance(q.sql.schemata, Select)
    assert _compare_str(
        q.sql.schemata,
        f"""
        SELECT nk_store.schema, count(DISTINCT nk_store.entity_id) AS count_1
        FROM nk_store {whereclause}
        GROUP BY nk_store.schema
        """,
    )

    assert isinstance(q.sql.countries, Select)
    assert _compare_str(
        q.sql.countries,
        f"""
        SELECT nk_store.value, count(DISTINCT nk_store.entity_id) AS count_1
        FROM nk_store
        WHERE nk_store.prop_type = :prop_type_1 AND nk_store.entity_id IN
        (SELECT nk_store.entity_id FROM nk_store {whereclause})
        GROUP BY nk_store.value
        """,
    )

    assert isinstance(q.sql.dates, Select)
    assert _compare_str(
        q.sql.dates,
        f"""
        SELECT min(nk_store.value) AS min_1, max(nk_store.value) AS max_1
        FROM nk_store
        WHERE nk_store.prop_type = :prop_type_1 AND nk_store.entity_id IN
        (SELECT nk_store.entity_id FROM nk_store {whereclause})
        """,
    )

    # order by creates a join
    q = (
        Query()
        .where(dataset="test")
        .where(dataset="other", schema="Event")
        .where(prop="date", value=2023, operator="gte")
        .order_by("name", ascending=False)
    )
    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields}, anon_1.entity_id AS entity_id_1, anon_1.value AS value_1
        FROM nk_store JOIN (SELECT DISTINCT nk_store.entity_id AS entity_id,
            nk_store.value AS value
            FROM nk_store
            WHERE nk_store.prop = :prop_1 AND nk_store.entity_id IN (SELECT nk_store.entity_id
                FROM nk_store
                WHERE (nk_store.dataset = :dataset_1 OR nk_store.dataset = :dataset_2)
                AND nk_store.schema = :schema_1
                AND nk_store.prop = :prop_2 AND nk_store.value >= :value_2))
        AS anon_1
        ON nk_store.entity_id = anon_1.entity_id
        ORDER BY anon_1.value DESC, nk_store.entity_id
        """,
    )

    # cast order by
    q = Query().order_by("amount")
    assert "CAST(nk_store.value AS NUMERIC)" in str(q.sql.statements)

    # no multi-value sort
    q = Query().order_by("name", "title")
    with pytest.raises(ValidationError):
        q.sql.statements

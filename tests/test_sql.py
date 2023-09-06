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
    whereclause = """WHERE (test_table.dataset = :dataset_1 OR test_table.dataset = :dataset_2)
    AND test_table.schema = :schema_1
    AND test_table.prop = :prop_1
    AND test_table.value >= :value_1"""
    fields = """test_table.id, test_table.entity_id, test_table.canonical_id, test_table.prop,
    test_table.prop_type, test_table.schema, test_table.value, test_table.original_value,
    test_table.dataset, test_table.lang, test_table.target, test_table.external,
    test_table.first_seen, test_table.last_seen"""
    assert isinstance(q.sql.canonical_ids, Select)
    assert _compare_str(
        q.sql.canonical_ids,
        f"""
        SELECT DISTINCT test_table.canonical_id
        FROM test_table {whereclause}
        """,
    )

    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM test_table
        WHERE test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id FROM test_table {whereclause})
        ORDER BY test_table.canonical_id
        """,
    )

    assert isinstance(q.sql.count, Select)
    assert _compare_str(
        q.sql.count,
        f"""
        SELECT count(DISTINCT test_table.canonical_id) AS count_1
        FROM test_table {whereclause}
        """,
    )

    assert isinstance(q.sql.datasets, Select)
    assert _compare_str(
        q.sql.datasets,
        f"""
        SELECT test_table.dataset, count(DISTINCT test_table.canonical_id) AS count_1
        FROM test_table {whereclause}
        GROUP BY test_table.dataset
        """,
    )

    assert isinstance(q.sql.schemata, Select)
    assert _compare_str(
        q.sql.schemata,
        f"""
        SELECT test_table.schema, count(DISTINCT test_table.canonical_id) AS count_1
        FROM test_table {whereclause}
        GROUP BY test_table.schema
        """,
    )

    assert isinstance(q.sql.countries, Select)
    assert _compare_str(
        q.sql.countries,
        f"""
        SELECT test_table.value, count(DISTINCT test_table.canonical_id) AS count_1
        FROM test_table
        WHERE test_table.prop_type = :prop_type_1 AND test_table.canonical_id IN
        (SELECT DISTINCT test_table.canonical_id FROM test_table {whereclause})
        GROUP BY test_table.value
        """,
    )

    assert isinstance(q.sql.dates, Select)
    assert _compare_str(
        q.sql.dates,
        f"""
        SELECT min(test_table.value) AS min_1, max(test_table.value) AS max_1
        FROM test_table
        WHERE test_table.prop_type = :prop_type_1 AND test_table.canonical_id IN
        (SELECT DISTINCT test_table.canonical_id FROM test_table {whereclause})
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
        SELECT {fields}, anon_1.canonical_id AS canonical_id_1, anon_1.sortable_value
        FROM test_table JOIN (SELECT test_table.canonical_id AS canonical_id, group_concat(test_table.value) AS sortable_value
            FROM test_table
            WHERE test_table.prop = :prop_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                FROM test_table WHERE (test_table.dataset = :dataset_1 OR test_table.dataset = :dataset_2)
                AND test_table.schema = :schema_1 AND test_table.prop = :prop_2 AND test_table.value >= :value_1)
            GROUP BY test_table.canonical_id
            ORDER BY sortable_value DESC, test_table.canonical_id)
        AS anon_1 ON test_table.canonical_id = anon_1.canonical_id
        ORDER BY anon_1.sortable_value DESC, test_table.canonical_id
        """,
    )

    # cast order by
    q = Query().order_by("amount")
    assert "CAST(test_table.value AS NUMERIC)" in str(q.sql.statements)

    # no multi-value sort
    q = Query().order_by("name", "title")
    with pytest.raises(ValidationError):
        q.sql.statements

    # slice
    q = (
        Query()
        .where(dataset="test")
        .where(dataset="other", schema="Event")
        .where(prop="date", value=2023, operator="gte")
    )
    assert str(q[:10].sql.canonical_ids).endswith("LIMIT :param_1")
    assert str(q[1:10].sql.canonical_ids).endswith("LIMIT :param_1 OFFSET :param_2")

    # ordered slice
    q = (
        Query()
        .where(dataset="test")
        .where(dataset="other", schema="Event")
        .where(prop="date", value=2023, operator="gte")
        .order_by("name")
    )
    assert not str(q[:10].sql.canonical_ids).endswith("LIMIT :param_1")
    assert not str(q[1:10].sql.canonical_ids).endswith("LIMIT :param_1 OFFSET :param_2")
    q = q[1:10]
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields}, anon_1.canonical_id AS canonical_id_1, anon_1.sortable_value
        FROM test_table JOIN (SELECT test_table.canonical_id AS canonical_id, group_concat(test_table.value) AS sortable_value
            FROM test_table
            WHERE test_table.prop = :prop_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                FROM test_table WHERE (test_table.dataset = :dataset_1 OR test_table.dataset = :dataset_2)
                AND test_table.schema = :schema_1 AND test_table.prop = :prop_2 AND test_table.value >= :value_1)
            GROUP BY test_table.canonical_id
            ORDER BY sortable_value, test_table.canonical_id
            LIMIT :param_1 OFFSET :param_2)
        AS anon_1 ON test_table.canonical_id = anon_1.canonical_id
        ORDER BY anon_1.sortable_value, test_table.canonical_id
        """,
    )

    # aggregation
    q = q.aggregate("sum", "amount").aggregate("max", "date")
    q = str(q.sql.aggregations)
    assert len(q.split("UNION")) == 2
    assert "SELECT 'date', 'max', max(test_table.value) AS max" in q
    assert "SELECT 'amount', 'sum', sum(test_table.value) AS sum" in q

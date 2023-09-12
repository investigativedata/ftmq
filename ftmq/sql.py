from functools import cached_property
from typing import TYPE_CHECKING

from followthemoney.model import registry
from nomenklatura.statement import make_statement_table
from sqlalchemy import (
    NUMERIC,
    BooleanClauseList,
    Column,
    MetaData,
    Select,
    and_,
    desc,
    func,
    or_,
    select,
    text,
    union_all,
)

from ftmq.enums import Comparators, PropertyTypes
from ftmq.exceptions import ValidationError
from ftmq.filters import F

if TYPE_CHECKING:
    from ftmq.query import Q


class Sql:
    COMPARATORS = {
        Comparators["eq"]: "__eq__",
        Comparators["not"]: "__ne__",
        Comparators["in"]: "in_",
        Comparators.null: "is_",
        Comparators.gt: "__gt__",
        Comparators.gte: "__ge__",
        Comparators.lt: "__lt__",
        Comparators.lte: "__le__",
    }

    def __init__(self, q: "Q") -> None:
        self.q = q
        self.metadata = MetaData()
        self.table = make_statement_table(self.metadata)

    def get_expression(self, column: Column, f: F):
        op = self.COMPARATORS.get(str(f.comparator), str(f.comparator))
        op = getattr(column, op)
        return op(f.value)

    @cached_property
    def clause(self) -> BooleanClauseList:
        clauses = []
        if self.q.datasets:
            clauses.append(
                or_(
                    self.get_expression(self.table.c.dataset, f)
                    for f in self.q.datasets
                )
            )
        if self.q.schemata:
            clauses.append(
                or_(
                    self.get_expression(self.table.c.schema, f) for f in self.q.schemata
                )
            )
        if self.q.reversed:
            rclause = or_(
                and_(
                    self.table.c.prop_type == str(registry.entity),
                    self.get_expression(self.table.c.value, f),
                )
                for f in self.q.reversed
            )
            rq = select(self.table.c.canonical_id.distinct()).where(
                and_(rclause, *clauses)
            )
            clauses.append(self.table.c.canonical_id.in_(rq))
        if self.q.properties:
            clauses.append(
                or_(
                    and_(
                        self.table.c.prop == f.key,
                        self.get_expression(self.table.c.value, f),
                    )
                    for f in self.q.properties
                )
            )
        return and_(*clauses)

    @cached_property
    def canonical_ids(self) -> Select:
        q = select(self.table.c.canonical_id.distinct()).where(self.clause)
        if self.q.sort is None:
            q = q.limit(self.q.limit).offset(self.q.offset)
        return q

    @cached_property
    def all_canonical_ids(self) -> Select:
        return self.canonical_ids.limit(None).offset(None)

    @cached_property
    def _unsorted_statements(self) -> Select:
        return (
            select(self.table)
            .where(self.table.c.canonical_id.in_(self.canonical_ids))
            .order_by(self.table.c.canonical_id)
        )

    @cached_property
    def _sorted_statements(self) -> Select:
        if self.q.sort:
            if len(self.q.sort.values) > 1:
                raise ValidationError(
                    f"Multi-valued sort not supported for `{self.__class__.__name__}`"
                )
            prop = self.q.sort.values[0]
            value = self.table.c.value
            if PropertyTypes[prop].value == registry.number:
                value = func.cast(self.table.c.value, NUMERIC)
            inner = (
                select(
                    self.table.c.canonical_id,
                    func.group_concat(value).label("sortable_value"),
                )
                .where(
                    and_(
                        self.table.c.prop == prop,
                        self.table.c.canonical_id.in_(self.canonical_ids),
                    )
                )
                .group_by(self.table.c.canonical_id)
                .limit(self.q.limit)
                .offset(self.q.offset)
            )

            order_by = "sortable_value"
            if not self.q.sort.ascending:
                order_by = desc(order_by)
            order_by = [order_by, self.table.c.canonical_id]

            inner = inner.order_by(*order_by)

            return select(
                self.table.join(
                    inner, self.table.c.canonical_id == inner.c.canonical_id
                )
            ).order_by(*order_by)

    @cached_property
    def statements(self) -> Select:
        if self.q.sort:
            return self._sorted_statements
        return self._unsorted_statements

    @cached_property
    def count(self) -> Select:
        return (
            select(func.count(self.table.c.canonical_id.distinct()))
            .select_from(self.table)
            .where(self.clause)
        )

    @cached_property
    def datasets(self) -> Select:
        return (
            select(
                self.table.c.dataset, func.count(self.table.c.canonical_id.distinct())
            )
            .where(self.clause)
            .group_by(self.table.c.dataset)
        )

    @cached_property
    def schemata(self) -> Select:
        return (
            select(
                self.table.c.schema, func.count(self.table.c.canonical_id.distinct())
            )
            .where(self.clause)
            .group_by(self.table.c.schema)
        )

    @cached_property
    def dates(self) -> Select:
        return select(
            func.min(self.table.c.value),
            func.max(self.table.c.value),
        ).where(
            self.table.c.prop_type == "date",
            self.table.c.canonical_id.in_(self.all_canonical_ids),
        )

    @cached_property
    def countries(self) -> Select:
        return (
            select(
                self.table.c.value,
                func.count(self.table.c.canonical_id.distinct()),
            )
            .where(
                self.table.c.prop_type == "country",
                self.table.c.canonical_id.in_(self.all_canonical_ids),
            )
            .group_by(self.table.c.value)
        )

    @cached_property
    def aggregations(self) -> Select:
        qs = []
        for agg in self.q.aggregations:
            qs.append(
                select(
                    text(f"'{agg.prop}'"),
                    text(f"'{agg.func}'"),
                    getattr(func, agg.func)(self.table.c.value),
                ).where(
                    self.table.c.prop == agg.prop,
                    self.table.c.canonical_id.in_(self.all_canonical_ids),
                )
            )
        return union_all(*qs)

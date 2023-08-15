from functools import cached_property
from typing import TYPE_CHECKING

from nomenklatura.db import get_statement_table
from sqlalchemy import and_, func, or_, select
from sqlalchemy.schema import Column
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.sql.selectable import Select

from ftmq.enums import Operators
from ftmq.filters import PropertyFilter

if TYPE_CHECKING:
    from ftmq.query import Q


class Sql:
    OPERATORS = {
        Operators["not"]: "__ne__",
        Operators["in"]: "in_",
        Operators.null: "is_",  # FIXME
        Operators.gt: "__gt__",
        Operators.gte: "__ge__",
        Operators.lt: "__lt__",
        Operators.lte: "__le__",
    }

    def __init__(self, q: "Q") -> None:
        self.q = q
        self.table = get_statement_table()

    def get_expression(self, column: Column, prop: PropertyFilter):
        if prop.operator is None:
            return column.__eq__(prop.casted_value)
        op = self.OPERATORS.get(str(prop.operator), str(prop.operator))
        op = getattr(column, op)
        return op(prop.casted_value)

    @cached_property
    def clause(self) -> BooleanClauseList:
        clauses = []
        if self.q.datasets:
            clauses.append(or_(self.table.c.dataset == str(d) for d in self.q.datasets))
        if self.q.schemata:
            clauses.append(or_(self.table.c.schema == str(s) for s in self.q.schemata))
        if self.q.properties:
            clauses.append(
                or_(
                    and_(
                        self.table.c.prop == str(p),
                        self.get_expression(self.table.c.value, p),
                    )
                    for p in self.q.properties
                )
            )
        return and_(*clauses)

    @cached_property
    def entity_ids(self) -> Select:
        # no distinct here for performance, distinct later if necessary
        return select(self.table.c.entity_id).where(self.clause)

    @cached_property
    def _unsorted_statements(self) -> Select:
        return (
            select(self.table)
            .where(self.table.c.entity_id.in_(self.entity_ids))
            .order_by(self.table.c.entity_id)
        )

    @cached_property
    def _sorted_statements(self) -> Select:
        if self.q.sort:
            prop = self.q.sort.values[0]  # FIXME
            inner = (
                select(self.table.c.entity_id, self.table.c.value)
                .where(
                    and_(
                        self.table.c.prop == prop,
                        self.table.c.entity_id.in_(self.entity_ids),
                    )
                )
                .distinct(self.table.c.entity_id)
            )
            order_by = inner.c.value
            if not self.q.sort.ascending:
                order_by = order_by.desc()

            return select(
                self.table.join(inner, self.table.c.entity_id == inner.c.entity_id)
            ).order_by(order_by, self.table.c.entity_id)

    @cached_property
    def statements(self) -> Select:
        if self.q.sort:
            return self._sorted_statements
        return self._unsorted_statements

    @property
    def count(self) -> Select:
        return (
            select(func.count(self.table.c.entity_id.distinct()))
            .select_from(self.table)
            .where(self.clause)
        )

    @property
    def datasets(self) -> Select:
        return (
            select(self.table.c.dataset, func.count(self.table.c.entity_id.distinct()))
            .where(self.clause)
            .group_by(self.table.c.dataset)
        )

    @property
    def schemata(self) -> Select:
        return (
            select(self.table.c.schema, func.count(self.table.c.entity_id.distinct()))
            .where(self.clause)
            .group_by(self.table.c.schema)
        )

    @property
    def dates(self) -> Select:
        return select(
            func.min(self.table.c.value),
            func.max(self.table.c.value),
        ).where(
            self.table.c.prop_type == "date",
            self.table.c.entity_id.in_(self.entity_ids),
        )

    @property
    def countries(self) -> Select:
        return (
            select(
                self.table.c.value,
                func.count(self.table.c.entity_id.distinct()),
            )
            .where(
                self.table.c.prop_type == "country",
                self.table.c.entity_id.in_(self.entity_ids),
            )
            .group_by(self.table.c.value)
        )

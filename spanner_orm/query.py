# python3
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helps build SQL for complex Spanner queries."""

from abc import ABC
from abc import abstractmethod

from spanner_orm.condition import ConditionSegment


class DatabaseQuery(ABC):
  """Helps build SQL for complex Spanner queries."""

  def __init__(self, model, conditions):
    self._model = model
    self._conditions = conditions
    self._parameters = None
    self._sql = None
    self._types = None

    self._build()

  def parameters(self):
    assert self._sql is not None
    return self._parameters

  def sql(self):
    assert self._sql is not None
    return self._sql

  def types(self):
    assert self._sql is not None
    return self._types

  @abstractmethod
  def parse_results(self, results):
    pass

  def _segments(self, segment_type):
    segments = [
        condition for condition in self._conditions
        if condition.segment() == segment_type
    ]
    for segment in segments:
      segment.bind(self._model)
    return segments

  def _build(self):
    """Builds the Spanner query from the given model and conditions."""
    segments = [
        self._select(),
        self._from(),
        self._where(),
        self._order(),
        self._limit()
    ]

    self._sql, self._parameters, self._types = '', {}, {}
    for segment in segments:
      segment_sql, segment_parameters, segment_types = segment
      self._sql += segment_sql

      self._update_unique(self._parameters, segment_parameters)
      self._update_unique(self._types, segment_types)

  @abstractmethod
  def _select(self):
    pass

  def _from(self):
    return (' FROM {}'.format(self._model.table()), {}, {})

  def _where(self):
    sql, sql_parts, parameters, types = '', [], {}, {}
    wheres = self._segments(ConditionSegment.WHERE)
    for where in wheres:
      sql_parts.append(where.sql())
      self._update_unique(parameters, where.params())
      self._update_unique(types, where.types())
    if sql_parts:
      sql = ' WHERE {}'.format(' AND '.join(sql_parts))
    return (sql, parameters, types)

  def _order(self):
    sql, parameters, types = '', {}, {}
    orders = self._segments(ConditionSegment.ORDER_BY)
    if orders:
      assert len(orders) == 1
      order = orders[0]
      sql = ' ' + order.sql()
      parameters = order.params()
      types = order.types()
    return (sql, parameters, types)

  def _limit(self):
    sql, parameters, types = '', {}, {}
    limits = self._segments(ConditionSegment.LIMIT)
    if limits:
      assert len(limits) == 1
      limit = limits[0]
      sql = ' ' + limit.sql()
      parameters = limit.params()
      types = limit.types()
    return (sql, parameters, types)

  @staticmethod
  def _update_unique(to_update, new_dict):
    assert to_update.keys().isdisjoint(new_dict)
    to_update.update(new_dict)


class CountQuery(DatabaseQuery):
  """Handles COUNT Spanner queries."""

  def _select(self):
    assert not self._segments(ConditionSegment.JOIN)
    return ('SELECT COUNT(*)', {}, {})

  def parse_results(self, results):
    return results[0][0]


class SelectQuery(DatabaseQuery):
  """Handles SELECT Spanner queries."""

  def _select_prefix(self):
    return 'SELECT'

  def _select(self):
    parameters, types = {}, {}
    columns = [
        '{alias}.{column}'.format(
            alias=self._model.column_prefix(), column=column)
        for column in self._model.columns()
    ]
    joins = self._segments(ConditionSegment.JOIN)
    for join in joins:
      subquery = _SelectSubQuery(join.destination(), join.conditions())

      self._update_unique(parameters, subquery.parameters())
      self._update_unique(types, subquery.types())

      columns.append('ARRAY({subquery})'.format(subquery=subquery.sql()))
    return ('{prefix} {columns}'.format(
        prefix=self._select_prefix(), columns=', '.join(columns)), parameters,
            types)

  def parse_results(self, results):
    models = []
    joins = self._segments(ConditionSegment.JOIN)
    for result in results:
      model, _ = self._parse_result(result, 0, self._model, joins)
      models.append(model)
    return models

  def _parse_result(self, row, offset, from_model, joins):
    """Parses a row of results from a Spanner query based on the conditions."""
    values = {}
    for column in from_model.columns():
      values[column] = row[offset]
      offset += 1
    for join in joins:
      subjoins = [
          condition for condition in join.conditions()
          if condition.segment() == ConditionSegment.JOIN
      ]

      model, new_offset = self._parse_result(row, offset, join.destination(),
                                             subjoins)
      values[join.relation_name()] = model
      offset = new_offset
    return (from_model(values, persisted=True), offset)


class _SelectSubQuery(SelectQuery):

  def _select_prefix(self):
    return 'SELECT AS STRUCT'

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

import abc

from spanner_orm import condition
from spanner_orm import error


class SpannerQuery(abc.ABC):
  """Helps build SQL for complex Spanner queries."""

  def __init__(self, model, conditions):
    self._model = model
    self._conditions = conditions
    self._param_offset = 0
    self._sql = ''
    self._parameters = {}
    self._types = {}
    self._build()

  def _param_index(self):
    return self._param_offset + len(self._parameters)

  def parameters(self):
    return self._parameters

  def sql(self):
    return self._sql

  def types(self):
    return self._types

  @abc.abstractmethod
  def process_results(self, results):
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
    segment_builders = [
        self._select,
        self._from,
        self._where,
        self._order,
        self._limit
    ]

    self._sql, self._parameters, self._types = '', {}, {}
    for segment_builder in segment_builders:
      segment_sql, segment_parameters, segment_types = segment_builder()
      self._sql += segment_sql
      self._parameters.update(segment_parameters)
      self._types.update(segment_types)

  @abc.abstractmethod
  def _select(self):
    """Processes the SELECT segment of the SQL query."""
    pass

  def _from(self):
    """Processes the FROM segment of the SQL query."""
    return (' FROM {}'.format(self._model.table()), {}, {})

  def _where(self):
    """Processes the WHERE segment of the SQL query."""
    sql, sql_parts, parameters, types = '', [], {}, {}
    wheres = self._segments(condition.Segment.WHERE)
    for where in wheres:
      where.suffix = str(self._param_index() + len(parameters))
      sql_parts.append(where.sql())
      parameters.update(where.params())
      types.update(where.types())
    if sql_parts:
      sql = ' WHERE {}'.format(' AND '.join(sql_parts))
    return (sql, parameters, types)

  def _order(self):
    """Processes the ORDER BY segment of the SQL query."""
    sql, parameters, types = '', {}, {}
    orders = self._segments(condition.Segment.ORDER_BY)
    if orders:
      if len(orders) != 1:
        raise error.SpannerError('Only one order condition may be specified')
      order = orders[0]
      order.suffix = str(self._param_index())
      sql = ' ' + order.sql()
      parameters = order.params()
      types = order.types()
    return (sql, parameters, types)

  def _limit(self):
    """Processes the LIMIT segment of the SQL query."""
    sql, parameters, types = '', {}, {}
    limits = self._segments(condition.Segment.LIMIT)
    if limits:
      if len(limits) != 1:
        raise error.SpannerError('Only one limit condition may be specified')
      limit = limits[0]
      limit.suffix = str(self._param_index())
      sql = ' ' + limit.sql()
      parameters = limit.params()
      types = limit.types()
    return (sql, parameters, types)


class CountQuery(SpannerQuery):
  """Handles COUNT Spanner queries."""

  def __init__(self, model, conditions):
    super().__init__(model, conditions)
    for c in conditions:
      if c.segment() != condition.Segment.WHERE:
        raise error.SpannerError('Only conditions that affect the WHERE clause '
                                 'are allowed for count queries')

  def _select(self):
    return ('SELECT COUNT(*)', {}, {})

  def process_results(self, results):
    return results[0][0]


class SelectQuery(SpannerQuery):
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
    joins = self._segments(condition.Segment.JOIN)
    for join in joins:
      subquery = _SelectSubQuery(join.destination, join.conditions,
                                 param_offset=self._param_index())

      columns.append('ARRAY({subquery})'.format(subquery=subquery.sql()))
      parameters.update(subquery.parameters())
      types.update(subquery.types())
    return ('{prefix} {columns}'.format(
        prefix=self._select_prefix(), columns=', '.join(columns)), parameters,
            types)

  def process_results(self, results):
    return [self._process_row(result) for result in results]

  def _process_row(self, row):
    """Parses a row of results from a Spanner query based on the conditions."""
    values = dict(zip(self._model.columns(), row))
    joins = self._segments(condition.Segment.JOIN)
    join_values = row[len(self._model.columns()):]
    for join, join_value in zip(joins, join_values):
      subquery = _SelectSubQuery(join.destination, join.conditions)
      models = subquery.process_results(join_value)
      if join.single:
        if len(models) > 1:
          raise error.SpannerError(
              'Multiple objects returned for relationship marked as single')
        values[join.relation_name] = models[0] if models else None
      else:
        values[join.relation_name] = models
    return self._model(values, persisted=True)


class _SelectSubQuery(SelectQuery):

  def __init__(self, model, conditions, param_offset=0):
    super().__init__(model, conditions)
    self._param_offset = param_offset

  def _select_prefix(self):
    return 'SELECT AS STRUCT'

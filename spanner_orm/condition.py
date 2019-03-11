# python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Used with Model#where and Model#count to help create Spanner queries."""

import abc
import enum

from spanner_orm import error

from google.cloud.spanner_v1.proto import type_pb2


class Segment(enum.Enum):
  """The segment of the SQL query that a Condition belongs to."""
  FROM = 1
  JOIN = 2
  WHERE = 3
  ORDER_BY = 4
  LIMIT = 5


class Condition(abc.ABC):
  """Base class for specifying conditions in a Spanner query."""

  def __init__(self):
    self.model = None
    self.suffix = None

  def bind(self, model):
    self._validate(model)
    self.model = model

  def key(self, name):
    if self.suffix:
      return '{name}{suffix}'.format(name=name, suffix=self.suffix)
    return name

  def params(self):
    if not self.model:
      raise error.SpannerError(
          'Condition must be bound before params is called')
    return self._params()

  @abc.abstractmethod
  def _params(self):
    pass

  @staticmethod
  @abc.abstractmethod
  def segment():
    raise NotImplementedError

  def sql(self):
    if not self.model:
      raise error.SpannerError('Condition must be bound before sql is called')
    return self._sql()

  @abc.abstractmethod
  def _sql(self):
    pass

  def types(self):
    if not self.model:
      raise error.SpannerError('Condition must be bound before types is called')
    return self._types()

  @abc.abstractmethod
  def _types(self):
    pass

  @abc.abstractmethod
  def _validate(self, model):
    pass


class ColumnsEqualCondition(Condition):
  """Used to join records by matching column values."""

  def __init__(self, origin_column, destination_model, destination_column):
    super().__init__()
    self.column = origin_column
    self.destination_model = destination_model
    self.destination_column = destination_column

  def _params(self):
    return {}

  def segment(self):
    return Segment.WHERE

  def _sql(self):
    return '{table}.{column} = {other_table}.{other_column}'.format(
        table=self.model.table,
        column=self.column,
        other_table=self.destination_model.table,
        other_column=self.destination_column)

  def _types(self):
    return {}

  def _validate(self, model):
    assert self.column in model.schema
    origin = model.schema[self.column]
    assert self.destination_column in self.destination_model.schema
    dest = self.destination_model.schema[self.destination_column]

    assert (origin.field_type() == dest.field_type() and
            origin.nullable() == dest.nullable())


class ForceIndexCondition(Condition):
  """Used to indicate which index should be used in a Spanner query."""

  def __init__(self, name):
    super().__init__()
    self.name = name
    self.index = None

  def bind(self, model):
    super().bind(model)
    self.index = self.model.indexes[self.name]

  def _params(self):
    return {}

  @staticmethod
  def segment():
    return Segment.FROM

  def _sql(self):
    return '@{{FORCE_INDEX={}}}'.format(self.index.name)

  def _types(self):
    return {}

  def _validate(self, model):
    assert self.name in model.indexes
    assert not model.indexes[self.name].primary


class IncludesCondition(Condition):
  """Used to include related models via a relation in a Spanner query."""

  def __init__(self, name, conditions=None):
    super().__init__()
    self.name = name
    self._conditions = conditions or []
    self.relation = None

  def bind(self, model):
    super().bind(model)
    self.relation = self.model.relations[self.name]

  @property
  def conditions(self):
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before conditions is called')
    return self.relation.conditions + self._conditions

  @property
  def destination(self):
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before destination is called')
    return self.relation.destination

  @property
  def relation_name(self):
    return self.name

  @property
  def single(self):
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before single is called')
    return self.relation.single

  def _params(self):
    return {}

  @staticmethod
  def segment():
    return Segment.JOIN

  def _sql(self):
    return ''

  def _types(self):
    return {}

  def _validate(self, model):
    assert self.name in model.relations
    other_model = model.relations[self.name].destination
    for condition in self._conditions:
      condition._validate(other_model)  # pylint: disable=protected-access


class LimitCondition(Condition):
  """Used to specify a LIMIT condition in a Spanner query."""

  def __init__(self, value, offset=0):
    super().__init__()
    for param in [value, offset]:
      if not isinstance(param, int):
        raise error.SpannerError(
            '{param} is not of type int'.format(param=param))

    self.limit = value
    self.offset = offset

  @property
  def _limit_key(self):
    return self.key('limit')

  @property
  def _offset_key(self):
    return self.key('offset')

  def _params(self):
    params = {self._limit_key: self.limit}
    if self.offset:
      params[self._offset_key] = self.offset
    return params

  @staticmethod
  def segment():
    return Segment.LIMIT

  def _sql(self):
    if self.offset:
      return 'LIMIT @{limit_key} OFFSET @{offset_key}'.format(
          limit_key=self._limit_key, offset_key=self._offset_key)
    return 'LIMIT @{limit_key}'.format(limit_key=self._limit_key)

  def _types(self):
    types = {self._limit_key: type_pb2.Type(code=type_pb2.INT64)}
    if self.offset:
      types[self._offset_key] = type_pb2.Type(code=type_pb2.INT64)
    return types

  def _validate(self, model):
    # Validation is independent of model for LIMIT
    del model


class OrCondition(Condition):
  """Used to join multiple conditions with an OR in a Spanner query."""

  def __init__(self, *condition_lists):
    super().__init__()
    if len(condition_lists) < 2:
      raise error.SpannerError(
          'OrCondition requires at least two lists of conditions')
    self.condition_lists = condition_lists
    self.all_conditions = []
    for conditions in condition_lists:
      self.all_conditions.extend(conditions)

  def bind(self, model):
    super().bind(model)
    for condition in self.all_conditions:
      condition.bind(model)

  def _params(self):
    result = {}
    for condition in self.all_conditions:
      condition.suffix = str(int(self.suffix or 0) + len(result))
      result.update(condition.params())
    return result

  def _sql(self):
    segments = []
    params = 0
    # Set proper suffix for each condition first
    for condition in self.all_conditions:
      condition.suffix = str(params + int(self.suffix or 0))
      params += len(condition.params())

    for conditions in self.condition_lists:
      new_segment = ' AND '.join([condition.sql() for condition in conditions])
      segments.append('({new_segment})'.format(new_segment=new_segment))
    return '({segments})'.format(segments=' OR '.join(segments))

  @staticmethod
  def segment():
    return Segment.WHERE

  def _types(self):
    result = {}
    for condition in self.all_conditions:
      condition.suffix = str(int(self.suffix or 0) + len(result))
      result.update(condition.types())
    return result

  def _validate(self, model):
    # condition is valid if all child conditions are valid
    del model


class OrderType(enum.Enum):
  ASC = 1
  DESC = 2


class OrderByCondition(Condition):
  """Used to specify an ORDER BY condition in a Spanner query."""

  def __init__(self, *orderings):
    super().__init__()
    for (_, order_type) in orderings:
      if not isinstance(order_type, OrderType):
        raise error.SpannerError(
            '{order} is not of type OrderType'.format(order=order_type))
    self.orderings = orderings

  def _params(self):
    return {}

  def _sql(self):
    orders = []
    for (column, order_type) in self.orderings:
      orders.append('{alias}.{column} {order_type}'.format(
          alias=self.model.column_prefix,
          column=column,
          order_type=order_type.name))
    return 'ORDER BY {orders}'.format(orders=', '.join(orders))

  @staticmethod
  def segment():
    return Segment.ORDER_BY

  def _types(self):
    return {}

  def _validate(self, model):
    for (column, _) in self.orderings:
      assert column in model.schema


class ComparisonCondition(Condition):
  """Used to specify a comparison between a column and a value in the WHERE."""
  _segment = Segment.WHERE

  def __init__(self, operator, column, value):
    super().__init__()
    self.operator = operator
    self.column = column
    self.value = value

  @property
  def _column_key(self):
    return self.key(self.column)

  def _params(self):
    return {self._column_key: self.value}

  @staticmethod
  def segment():
    return Segment.WHERE

  def _sql(self):
    return '{alias}.{column} {operator} @{column_key}'.format(
        alias=self.model.column_prefix,
        column=self.column,
        operator=self.operator,
        column_key=self._column_key)

  def _types(self):
    return {self._column_key: self.model.schema[self.column].grpc_type()}

  def _validate(self, model):
    schema = model.schema
    assert self.column in schema
    assert self.value is not None
    schema[self.column].validate(self.value)


class ListComparisonCondition(ComparisonCondition):
  """Used to compare between a column and a list of values."""

  def _sql(self):
    return '{alias}.{column} {operator} UNNEST(@{column_key})'.format(
        alias=self.model.column_prefix,
        column=self.column,
        operator=self.operator,
        column_key=self._column_key)

  def _types(self):
    grpc_type = self.model.schema[self.column].grpc_type()
    list_type = type_pb2.Type(code=type_pb2.ARRAY, array_element_type=grpc_type)
    return {self._column_key: list_type}

  def _validate(self, model):
    schema = model.schema
    assert isinstance(self.value, list)
    assert self.column in schema
    for value in self.value:
      schema[self.column].validate(value)


class NullableComparisonCondition(ComparisonCondition):
  """Used to compare between a nullable column and a value or None."""

  def __init__(self, operator, nullable_operator, column, value):
    super().__init__(operator, column, value)
    self.nullable_operator = nullable_operator

  def is_null(self):
    return self.value is None

  def _params(self):
    if self.is_null():
      return {}
    return super()._params()

  def _sql(self):
    if self.is_null():
      return '{alias}.{column} {operator} NULL'.format(
          alias=self.model.column_prefix,
          column=self.column,
          operator=self.nullable_operator)
    return super()._sql()

  def _types(self):
    if self.is_null():
      return {}
    return super()._types()

  def _validate(self, model):
    schema = model.schema
    assert self.column in schema
    schema[self.column].validate(self.value)


class EqualityCondition(NullableComparisonCondition):
  """Represents an equality comparison in a Spanner query."""

  def __init__(self, column, value):
    super().__init__('=', 'IS', column, value)

  def __eq__(self, obj):
    return isinstance(obj, EqualityCondition) and self.value == obj.value


class InequalityCondition(NullableComparisonCondition):
  """Represents an inequality comparison in a Spanner query."""

  def __init__(self, column, value):
    super().__init__('!=', 'IS NOT', column, value)


def columns_equal(origin_column, dest_model, dest_column):
  return ColumnsEqualCondition(origin_column, dest_model, dest_column)


def equal_to(column, value):
  return EqualityCondition(column, value)


def force_index(index):
  return ForceIndexCondition(index)


def greater_than(column, value):
  return ComparisonCondition('>', column, value)


def greater_than_or_equal_to(column, value):
  return ComparisonCondition('>=', column, value)


def includes(relation, conditions=None):
  return IncludesCondition(relation, conditions)


def in_list(column, value):
  return ListComparisonCondition('IN', column, value)


def less_than(column, value):
  return ComparisonCondition('<', column, value)


def less_than_or_equal_to(column, value):
  return ComparisonCondition('<=', column, value)


def limit(value, offset=0):
  return LimitCondition(value, offset=offset)


def not_equal_to(column, value):
  return InequalityCondition(column, value)


def not_greater_than(column, value):
  return less_than_or_equal_to(column, value)


def not_in_list(column, value):
  return ListComparisonCondition('NOT IN', column, value)


def not_less_than(column, value):
  return greater_than_or_equal_to(column, value)


def or_(*condition_lists):
  return OrCondition(*condition_lists)


def order_by(*orderings):
  return OrderByCondition(*orderings)

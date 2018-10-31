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

# python3
"""Used with Model#where and Model#count to help create Spanner queries"""

from abc import ABC
from abc import abstractmethod
from enum import Enum

from google.cloud.spanner_v1.proto import type_pb2


class ConditionSegment(Enum):
  WHERE = 1
  ORDER_BY = 2
  LIMIT = 3
  JOIN = 4


class Condition(ABC):
  """Base class for specifying conditions in a Spanner query"""

  def __init__(self):
    self.model = None

  def bind(self, model):
    self._validate(model)
    self.model = model

  def params(self):
    assert self.model
    return self._params()

  @abstractmethod
  def _params(self):
    pass

  @staticmethod
  @abstractmethod
  def segment():
    raise NotImplementedError

  def sql(self):
    assert self.model
    return self._sql()

  @abstractmethod
  def _sql(self):
    pass

  def types(self):
    assert self.model
    return self._types()

  @abstractmethod
  def _types(self):
    pass

  @abstractmethod
  def _validate(self, model):
    pass


class ColumnsEqualCondition(Condition):
  """Used to join records by matching column values"""

  def __init__(self, origin_column, destination_model, destination_column):
    super().__init__()
    self.column = origin_column
    self.destination_model = destination_model
    self.destination_column = destination_column

  def _params(self):
    return {}

  def segment(self):
    return ConditionSegment.WHERE

  def _sql(self):
    return '{table}.{column} = {other_table}.{other_column}'.format(
        table=self.model.table(),
        column=self.column,
        other_table=self.destination_model.table(),
        other_column=self.destination_column)

  def _types(self):
    return {}

  def _validate(self, model):
    assert self.column in model.schema()
    origin_type = model.schema()[self.column]
    assert self.destination_column in self.destination_model.schema()
    destination_type = self.destination_model.schema()[self.destination_column]

    assert origin_type.db_type() == destination_type.db_type()


class IncludesCondition(Condition):
  """Used to include related models via a relation in a Spanner query"""

  def __init__(self, name, conditions=None):
    super().__init__()
    self.name = name
    self._conditions = conditions or []
    self.relation = None

  def bind(self, model):
    super().bind(model)
    self.relation = self.model.relations()[self.name]

  def conditions(self):
    assert self.relation
    return self.relation.conditions() + self._conditions

  def destination(self):
    assert self.relation
    return self.relation.destination()

  def relation_name(self):
    return self.name

  def _params(self):
    return {}

  @staticmethod
  def segment():
    return ConditionSegment.JOIN

  def _sql(self):
    return ''

  def _types(self):
    return {}

  def _validate(self, model):
    assert self.name in model.relations()
    other_model = model.relations()[self.name].destination()
    for condition in self._conditions:
      condition._validate(other_model)  # pylint: disable=protected-access


class LimitCondition(Condition):
  """Used to specify a LIMIT condition in a Spanner query"""
  KEY = 'limit'

  def __init__(self, value):
    super().__init__()
    assert isinstance(value, int)
    self.value = value

  def _params(self):
    return {self.KEY: self.value}

  @staticmethod
  def segment():
    return ConditionSegment.LIMIT

  def _sql(self):
    return 'LIMIT @{key}'.format(key=self.KEY)

  def _types(self):
    return {self.KEY: type_pb2.Type(code=type_pb2.INT64)}

  def _validate(self, model):
    # Validation is independent of model for LIMIT
    del model


class OrderType(Enum):
  ASC = 1
  DESC = 2


class OrderByCondition(Condition):
  """Used to specify an ORDER BY condition in a Spanner query"""

  def __init__(self, *orderings):
    super().__init__()
    for (_, order_type) in orderings:
      assert isinstance(order_type, OrderType)
    self.orderings = orderings

  def _params(self):
    return {}

  def _sql(self):
    orders = []
    for (column, order_type) in self.orderings:
      orders.append('{alias}.{column} {order_type}'.format(
          alias=self.model.column_prefix(),
          column=column,
          order_type=order_type.name))
    return 'ORDER BY {orders}'.format(orders=', '.join(orders))

  @staticmethod
  def segment():
    return ConditionSegment.ORDER_BY

  def _types(self):
    return {}

  def _validate(self, model):
    for (column, _) in self.orderings:
      assert column in model.schema()


class ComparisonCondition(Condition):
  """Used to specify a comparison between a column and a value in the WHERE"""
  _segment = ConditionSegment.WHERE

  def __init__(self, column, value):
    super().__init__()
    self.column = column
    self.value = value

  @staticmethod
  @abstractmethod
  def operator():
    raise NotImplementedError

  def _params(self):
    return {self.column: self.value}

  @staticmethod
  def segment():
    return ConditionSegment.WHERE

  def _sql(self):
    return '{alias}.{column} {operator} @{column}'.format(
        alias=self.model.column_prefix(),
        column=self.column,
        operator=self.operator())

  def _types(self):
    return {self.column: self.model.schema()[self.column].grpc_type()}

  def _validate(self, model):
    schema = model.schema()
    assert self.column in schema
    assert self.value is not None
    schema[self.column].validate(self.value)


class GreaterThanCondition(ComparisonCondition):

  @staticmethod
  def operator():
    return '>'


class GreaterThanOrEqualCondition(ComparisonCondition):

  @staticmethod
  def operator():
    return '>='


class LessThanCondition(ComparisonCondition):

  @staticmethod
  def operator():
    return '<'


class LessThanOrEqualCondition(ComparisonCondition):

  @staticmethod
  def operator():
    return '<='


class ListComparisonCondition(ComparisonCondition):
  """Used to compare between a column and a list of values"""

  def _sql(self):
    return '{alias}.{column} {operator} UNNEST(@{column})'.format(
        alias=self.model.column_prefix(),
        column=self.column,
        operator=self.operator())

  def _types(self):
    return {self.column: self.model.schema()[self.column].grpc_list_type()}

  def _validate(self, model):
    schema = model.schema()
    assert isinstance(self.value, list)
    assert self.column in schema
    for value in self.value:
      schema[self.column].validate(value)


class InListCondition(ListComparisonCondition):

  @staticmethod
  def operator():
    return 'IN'


class NotInListCondition(ListComparisonCondition):

  @staticmethod
  def operator():
    return 'NOT IN'


class NullableComparisonCondition(ComparisonCondition):
  """Used to compare between a nullable column and a value or None"""

  def is_null(self):
    return self.value is None

  @staticmethod
  @abstractmethod
  def nullable_operator():
    raise NotImplementedError

  def _params(self):
    if self.is_null():
      return {}
    return super()._params()

  def _sql(self):
    if self.is_null():
      return '{alias}.{column} {operator} NULL'.format(
          alias=self.model.column_prefix(),
          column=self.column,
          operator=self.nullable_operator())
    return super()._sql()

  def _types(self):
    if self.is_null():
      return {}
    return super()._types()

  def _validate(self, model):
    schema = model.schema()
    assert self.column in schema
    schema[self.column].validate(self.value)


class EqualityCondition(NullableComparisonCondition):
  """Represents an equality comparison in a Spanner query"""

  def __eq__(self, obj):
    return isinstance(obj, EqualityCondition) and self.value == obj.value

  def nullable_operator(self):
    return 'IS'

  @staticmethod
  def operator():
    return '='


class InequalityCondition(NullableComparisonCondition):

  @staticmethod
  def nullable_operator():
    return 'IS NOT'

  @staticmethod
  def operator():
    return '!='


def columns_equal(origin_column, dest_model, dest_column):
  return ColumnsEqualCondition(origin_column, dest_model, dest_column)


def equal_to(column, value):
  return EqualityCondition(column, value)


def greater_than(column, value):
  return GreaterThanCondition(column, value)


def greater_than_or_equal_to(column, value):
  return GreaterThanOrEqualCondition(column, value)


def includes(relation, conditions=None):
  return IncludesCondition(relation, conditions)


def in_list(column, value):
  return InListCondition(column, value)


def less_than(column, value):
  return LessThanCondition(column, value)


def less_than_or_equal_to(column, value):
  return LessThanOrEqualCondition(column, value)


def limit(value):
  return LimitCondition(value)


def not_equal_to(column, value):
  return InequalityCondition(column, value)


def not_greater_than(column, value):
  return less_than_or_equal_to(column, value)


def not_in_list(column, value):
  return NotInListCondition(column, value)


def not_less_than(column, value):
  return greater_than_or_equal_to(column, value)


def order_by(*orderings):
  return OrderByCondition(*orderings)

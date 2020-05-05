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
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

from spanner_orm import error
from spanner_orm import field
from spanner_orm import index
from spanner_orm import relationship

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
    self.model_class = None  # type: Optional[Type[Any]]
    self.suffix = None  # type: Optional[str]

  def bind(self, model_class: Type[Any]) -> None:
    """Specifies which model instance the condition is being run on."""
    self._validate(model_class)
    self.model_class = model_class

  def key(self, name: str) -> str:
    """Returns the unique parameter name for the given name.

    When a name is used multiple times by different conditions (for instance,
    we name parameters for the column they are being compared against, so
    multiple conditions on the same column causes this), we need to generate
    a unique name to disambiguate between these parameters. We do that by
    appending a suffix that is based on the number of parameters that have
    already been added to the query

    Args:
      name: Name of parameter to make unique
    """
    if self.suffix:
      return '{name}{suffix}'.format(name=name, suffix=self.suffix)
    return name

  def params(self) -> Dict[str, Any]:
    """Returns parameters to be used in the SQL query.

    Returns:
      Dictionary mapping from parameter name to value that should be
      substituted for that parameter in the SQL query
    """
    if not self.model_class:
      raise error.SpannerError('Condition must be bound before usage')
    return self._params()

  @abc.abstractmethod
  def _params(self) -> Dict[str, Any]:
    raise NotImplementedError

  @abc.abstractmethod
  def segment(self) -> Segment:
    """Returns which segment of the SQL query this condition belongs to."""
    raise NotImplementedError

  def sql(self) -> str:
    """Generates and returns the SQL to be used in the Spanner query."""

    if not self.model_class:
      raise error.SpannerError('Condition must be bound before usage')
    return self._sql()

  @abc.abstractmethod
  def _sql(self) -> str:
    pass

  def types(self) -> Dict[str, type_pb2.Type]:
    """Returns parameter types to be used in the SQL query.

    Returns:
      Dictionary mapping from parameter name to the type of the value that
      should be substituted for that parameter in the SQL query
    """
    if not self.model_class:
      raise error.SpannerError('Condition must be bound before usage')
    return self._types()

  @abc.abstractmethod
  def _types(self) -> Dict[str, type_pb2.Type]:
    raise NotImplementedError

  @abc.abstractmethod
  def _validate(self, model_class: Type[Any]) -> None:
    raise NotImplementedError


class ColumnsEqualCondition(Condition):
  """Used to join records by matching column values."""

  def __init__(self, origin_column: str, destination_model_class: Type[Any],
               destination_column: str):
    super().__init__()
    self.column = origin_column
    self.destination_model_class = destination_model_class
    self.destination_column = destination_column

  def _params(self) -> Dict[str, Any]:
    return {}

  def segment(self) -> Segment:
    return Segment.WHERE

  def _sql(self) -> str:
    return '{table}.{column} = {other_table}.{other_column}'.format(
        table=self.model_class.table,
        column=self.column,
        other_table=self.destination_model_class.table,
        other_column=self.destination_column)

  def _types(self) -> Dict[str, type_pb2.Type]:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    if self.column not in model_class.fields:
      raise error.ValidationError('{} is not a column on {}'.format(
          self.column, model_class.table))
    origin = model_class.fields[self.column]
    if self.destination_column not in self.destination_model_class.fields:
      raise error.ValidationError('{} is not a column on {}'.format(
          self.destination_column, self.destination_model_class.table))
    dest = self.destination_model_class.fields[self.destination_column]

    if (origin.field_type != dest.field_type or
        origin.nullable != dest.nullable):
      raise error.ValidationError('Types of {} and {} do not match'.format(
          origin.name, dest.name))


class ForceIndexCondition(Condition):
  """Used to indicate which index should be used in a Spanner query."""

  def __init__(self, index_or_name: Union[Type[index.Index], str]):
    super().__init__()
    if isinstance(index_or_name, index.Index):
      self.name = index_or_name.name
      self.index = index_or_name
    else:
      self.name = index_or_name
      self.index = None

  def bind(self, model_class: Type[Any]) -> None:
    super().bind(model_class)
    self.index = self.model_class.indexes[self.name]

  def _params(self) -> Dict[str, Any]:
    return {}

  def segment(self) -> Segment:
    return Segment.FROM

  def _sql(self) -> str:
    return '@{{FORCE_INDEX={}}}'.format(self.name)

  def _types(self) -> Dict[str, type_pb2.Type]:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    if self.name not in model_class.indexes:
      raise error.ValidationError('{} is not an index on {}'.format(
          self.name, model_class.table))
    if self.index and self.index != model_class.indexes[self.name]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.index.name, model_class.table))

    if model_class.indexes[self.name].primary:
      raise error.ValidationError('Cannot force query using primary index')


class IncludesCondition(Condition):
  """Used to include related model_classs via a relation in a Spanner query."""

  def __init__(self,
               relation_or_name: Union[relationship.Relationship, str],
               conditions: List[Condition] = None):
    super().__init__()
    if isinstance(relation_or_name, relationship.Relationship):
      self.name = relation_or_name.name
      self.relation = relation_or_name
    else:
      self.name = relation_or_name
      self.relation = None
    self._conditions = conditions or []

  def bind(self, model_class: Type[Any]) -> None:
    super().bind(model_class)
    self.relation = self.model_class.relations[self.name]

  @property
  def conditions(self) -> List[Condition]:
    """Generate the child conditions based on the relationship constraints."""
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before conditions is called')
    relation_conditions = []
    for constraint in self.relation.constraints:
      # This is backward from what you might imagine because the condition will
      # be processed from the context of the destination model
      relation_conditions.append(
          ColumnsEqualCondition(constraint.destination_column,
                                constraint.origin_class,
                                constraint.origin_column))
    return relation_conditions + self._conditions

  @property
  def destination(self) -> Type[Any]:
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before destination is called')
    return self.relation.destination

  @property
  def relation_name(self) -> str:
    return self.name

  @property
  def single(self) -> bool:
    if not self.relation:
      raise error.SpannerError(
          'Condition must be bound before single is called')
    return self.relation.single

  def _params(self) -> Dict[str, Any]:
    return {}

  def segment(self) -> Segment:
    return Segment.JOIN

  def _sql(self) -> str:
    return ''

  def _types(self) -> Dict[str, type_pb2.Type]:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    if self.name not in model_class.relations:
      raise error.ValidationError('{} is not a relation on {}'.format(
          self.name, model_class.table))
    if self.relation and self.relation != model_class.relations[self.name]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.relation.name, model_class.table))

    other_model_class = model_class.relations[self.name].destination
    for condition in self._conditions:
      condition._validate(other_model_class)  # pylint: disable=protected-access


class LimitCondition(Condition):
  """Used to specify a LIMIT condition in a Spanner query."""

  def __init__(self, value: int, offset: int = 0):
    super().__init__()
    for param in [value, offset]:
      if not isinstance(param, int):
        raise error.SpannerError(
            '{param} is not of type int'.format(param=param))

    self.limit = value
    self.offset = offset

  @property
  def _limit_key(self) -> str:
    return self.key('limit')

  @property
  def _offset_key(self) -> str:
    return self.key('offset')

  def _params(self) -> Dict[str, Any]:
    params = {self._limit_key: self.limit}
    if self.offset:
      params[self._offset_key] = self.offset
    return params

  def segment(self) -> Segment:
    return Segment.LIMIT

  def _sql(self) -> str:
    if self.offset:
      return 'LIMIT @{limit_key} OFFSET @{offset_key}'.format(
          limit_key=self._limit_key, offset_key=self._offset_key)
    return 'LIMIT @{limit_key}'.format(limit_key=self._limit_key)

  def _types(self) -> Dict[str, type_pb2.Type]:
    types = {self._limit_key: type_pb2.Type(code=type_pb2.INT64)}
    if self.offset:
      types[self._offset_key] = type_pb2.Type(code=type_pb2.INT64)
    return types

  def _validate(self, model_class: Type[Any]) -> None:
    # Validation is independent of model_class for LIMIT
    del model_class


class OrCondition(Condition):
  """Used to join multiple conditions with an OR in a Spanner query."""

  def __init__(self, *condition_lists: List[Condition]):
    super().__init__()
    if len(condition_lists) < 2:
      raise error.SpannerError(
          'OrCondition requires at least two lists of conditions')
    self.condition_lists = condition_lists
    self.all_conditions = []
    for conditions in condition_lists:
      self.all_conditions.extend(conditions)

  def bind(self, model_class: Type[Any]) -> None:
    super().bind(model_class)
    for condition in self.all_conditions:
      condition.bind(model_class)

  def _params(self) -> Dict[str, Any]:
    result = {}
    for condition in self.all_conditions:
      condition.suffix = str(int(self.suffix or 0) + len(result))
      result.update(condition.params())
    return result

  def _sql(self) -> str:
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

  def segment(self) -> Segment:
    return Segment.WHERE

  def _types(self) -> type_pb2.Type:
    result = {}
    for condition in self.all_conditions:
      condition.suffix = str(int(self.suffix or 0) + len(result))
      result.update(condition.types())
    return result

  def _validate(self, model_class: Type[Any]) -> None:
    # condition is valid if all child conditions are valid
    del model_class


class OrderType(enum.Enum):
  ASC = 1
  DESC = 2


class OrderByCondition(Condition):
  """Used to specify an ORDER BY condition in a Spanner query."""

  def __init__(self, *orderings: Tuple[Union[field.Field, str], OrderType]):
    super().__init__()
    for (_, order_type) in orderings:
      if not isinstance(order_type, OrderType):
        raise error.SpannerError(
            '{order} is not of type OrderType'.format(order=order_type))
    self.orderings = orderings

  def _params(self) -> Dict[str, Any]:
    return {}

  def _sql(self) -> str:
    orders = []
    for (column, order_type) in self.orderings:
      if isinstance(column, field.Field):
        column = column.name
      orders.append('{alias}.{column} {order_type}'.format(
          alias=self.model_class.column_prefix,
          column=column,
          order_type=order_type.name))
    return 'ORDER BY {orders}'.format(orders=', '.join(orders))

  def segment(self) -> Segment:
    return Segment.ORDER_BY

  def _types(self) -> type_pb2.Type:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    for (column, _) in self.orderings:
      if isinstance(column, field.Field):
        column = column.name
      if column not in model_class.fields:
        raise error.ValidationError('{} is not a column on {}'.format(
            column, model_class.table))


class ComparisonCondition(Condition):
  """Used to specify a comparison between a column and a value in the WHERE."""
  _segment = Segment.WHERE

  def __init__(self, operator: str, field_or_name: Union[field.Field, str],
               value: Any):
    super().__init__()
    self.operator = operator
    self.value = value
    if isinstance(field_or_name, field.Field):
      self.column = field_or_name.name
      self.field = field_or_name
    else:
      self.column = field_or_name
      self.field = None

  @property
  def _column_key(self) -> str:
    return self.key(self.column)

  def _params(self) -> Dict[str, Any]:
    return {self._column_key: self.value}

  def segment(self) -> Segment:
    return Segment.WHERE

  def _sql(self) -> str:
    return '{alias}.{column} {operator} @{column_key}'.format(
        alias=self.model_class.column_prefix,
        column=self.column,
        operator=self.operator,
        column_key=self._column_key)

  def _types(self) -> type_pb2.Type:
    return {self._column_key: self.model_class.fields[self.column].grpc_type()}

  def _validate(self, model_class: Type[Any]) -> None:
    if self.column not in model_class.fields:
      raise error.ValidationError('{} is not a column on {}'.format(
          self.column, model_class.table))
    if self.field and self.field != model_class.fields[self.column]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.column, model_class.table))
    if self.value is None:
      raise error.ValidationError('{} does not support NULL'.format(
          self.__name__))
    model_class.fields[self.column].validate(self.value)


class ListComparisonCondition(ComparisonCondition):
  """Used to compare between a column and a list of values."""

  def _sql(self) -> str:
    return '{alias}.{column} {operator} UNNEST(@{column_key})'.format(
        alias=self.model_class.column_prefix,
        column=self.column,
        operator=self.operator,
        column_key=self._column_key)

  def _types(self) -> type_pb2.Type:
    grpc_type = self.model_class.fields[self.column].grpc_type()
    list_type = type_pb2.Type(code=type_pb2.ARRAY, array_element_type=grpc_type)
    return {self._column_key: list_type}

  def _validate(self, model_class: Type[Any]) -> None:
    if not isinstance(self.value, list):
      raise error.ValidationError('{} is not a list'.format(self.value))
    if self.column not in model_class.fields:
      raise error.ValidationError('{} is not a column on {}'.format(
          self.column, model_class.table))
    if self.field and self.field != model_class.fields[self.column]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.column, model_class.table))
    for value in self.value:
      model_class.fields[self.column].validate(value)


class NullableComparisonCondition(ComparisonCondition):
  """Used to compare between a nullable column and a value or None."""

  def __init__(self, operator: str, nullable_operator: str,
               column: Union[field.Field, str], value: Any):
    super().__init__(operator, column, value)
    self.nullable_operator = nullable_operator

  def is_null(self) -> bool:
    return self.value is None

  def _params(self) -> Dict[str, Any]:
    if self.is_null():
      return {}
    return super()._params()

  def _sql(self) -> str:
    if self.is_null():
      return '{alias}.{column} {operator} NULL'.format(
          alias=self.model_class.column_prefix,
          column=self.column,
          operator=self.nullable_operator)
    return super()._sql()

  def _types(self) -> type_pb2.Type:
    if self.is_null():
      return {}
    return super()._types()

  def _validate(self, model_class: Type[Any]) -> None:
    if self.column not in model_class.fields:
      raise error.ValidationError('{} is not a column on {}'.format(
          self.column, model_class.table))
    if self.field and self.field != model_class.fields[self.column]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.column, model_class.table))
    model_class.fields[self.column].validate(self.value)


class EqualityCondition(NullableComparisonCondition):
  """Represents an equality comparison in a Spanner query."""

  def __init__(self, column: Union[field.Field, str], value: Any):
    super().__init__('=', 'IS', column, value)

  def __eq__(self, obj: Any) -> bool:
    return isinstance(obj, EqualityCondition) and self.value == obj.value


class InequalityCondition(NullableComparisonCondition):
  """Represents an inequality comparison in a Spanner query."""

  def __init__(self, column: Union[field.Field, str], value: Any):
    super().__init__('!=', 'IS NOT', column, value)


def columns_equal(origin_column: str, dest_model_class: Type[Any],
                  dest_column: str) -> ColumnsEqualCondition:
  """Condition where the specified columns are equal.

  Used in 'includes' to fetch related models where the foreign key matches the
  local value.

  Args:
    origin_column: Name of the column on the origin model to compare from
    dest_model_class: Type of model that is being compared to
    dest_column: Name of column on the destination model being compared to

  Returns:
    A Condition subclass that will be used in the query
  """
  return ColumnsEqualCondition(origin_column, dest_model_class, dest_column)


def equal_to(column: Union[field.Field, str], value: Any) -> EqualityCondition:
  """Condition where the specified column is equal to the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return EqualityCondition(column, value)


def force_index(forced_index: Union[index.Index, str]) -> ForceIndexCondition:
  """Condition to force the query to use the given index.

  Args:
    forced_index: Name of the index on the origin model or the Index on the
      origin model class to use

  Returns:
    A Condition subclass that will be used in the query
  """
  return ForceIndexCondition(forced_index)


def greater_than(column: Union[field.Field, str],
                 value: Any) -> ComparisonCondition:
  """Condition where the specified column is greater than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """

  return ComparisonCondition('>', column, value)


def greater_than_or_equal_to(column: Union[field.Field, str],
                             value: Any) -> ComparisonCondition:
  """Condition where the specified column is not less than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return ComparisonCondition('>=', column, value)


def includes(relation: Union[relationship.Relationship, str],
             conditions: List[Condition] = None) -> IncludesCondition:
  """Condition where the objects associated with a relationship are retrieved.

  Note that the query formed by this call is not a JOIN, but instead a
  subquery on the table on the other side of the relationship. To narrow
  down the objects retrieved in the subquery, conditions that apply to that
  subquery may be included, but not all conditions may apply

  Args:
    relation: Name of the relationship on the origin model or the Relationship
      on the origin model class used to retrievec associated objects
    conditions: Conditions to apply on the subquery

  Returns:
    A Condition subclass that will be used in the query
  """
  return IncludesCondition(relation, conditions)


def in_list(column: Union[field.Field, str],
            values: Iterable[Any]) -> ListComparisonCondition:
  """Condition where the specified column matches a value from the given list.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    values: A list of values. Any row for which the specified column matches a
      value in this list will be included in the result set.

  Returns:
    A Condition subclass that will be used in the query
  """
  return ListComparisonCondition('IN', column, values)


def less_than(column: Union[field.Field, str],
              value: Any) -> ComparisonCondition:
  """Condition where the specified column is less than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return ComparisonCondition('<', column, value)


def less_than_or_equal_to(column: Union[field.Field, str],
                          value: Any) -> ComparisonCondition:
  """Condition where the specified column is not greater than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return ComparisonCondition('<=', column, value)


def limit(value: int, offset: int = 0) -> LimitCondition:
  """Condition that specifies LIMIT and OFFSET of the query.

  Args:
    value: Ceiling on number of results in the result set
    offset: The index of the first item in the results to include in the result
      set

  Returns:
    A Condition subclass that will be used in the query
  """
  return LimitCondition(value, offset=offset)


def not_equal_to(column: Union[field.Field, str],
                 value: Any) -> InequalityCondition:
  """Condition where the specified column is not equal to the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return InequalityCondition(column, value)


def not_greater_than(column: Union[field.Field, str],
                     value: Any) -> ComparisonCondition:
  """Condition where the specified column is not greater than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return less_than_or_equal_to(column, value)


def not_in_list(column: Union[field.Field, str],
                values: List[Any]) -> ListComparisonCondition:
  """Condition where the specified column does not matche a value from the list.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    values: A list of values. Any row for which the specified column does not
      match any values in this list will be included in the result set.

  Returns:
    A Condition subclass that will be used in the query
  """
  return ListComparisonCondition('NOT IN', column, values)


def not_less_than(column: Union[field.Field, str],
                  value: Any) -> ComparisonCondition:
  """Condition where the specified column is not less than the given value.

  Args:
    column: Name of the column on the origin model or the Field on the origin
      model class to compare from
    value: The value to compare against

  Returns:
    A Condition subclass that will be used in the query
  """
  return greater_than_or_equal_to(column, value)


def or_(*condition_lists: List[Condition]) -> OrCondition:
  """Condition allows more complicated OR queries.

  Args:
    *condition_lists: Each value is a list of conditions that are combined using
      AND (which is also the default when using Model.where) All the values will
      then be combined using OR.

  Returns:
    A Condition subclass that will be used in the query
  """
  return OrCondition(*condition_lists)


def order_by(
    *orderings: Tuple[Union[field.Field, str], OrderType]) -> OrderByCondition:
  """Condition that specifies the ordering of the result set.

  Args:
    *orderings: A list of tuples. The first item in each tuple is the name of
      the column on the model or the Field on the model class which is being
      ordered. The second item is the OrderType to use (which is either
      ascending or descending). The index in the list indicates the position of
      that ordering in the resulting ORDER BY statement

  Returns:
    A Condition subclass that will be used in the query
  """
  return OrderByCondition(*orderings)

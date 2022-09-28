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
import base64
import dataclasses
import datetime
import decimal
import enum
import string
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union

from spanner_orm import error
from spanner_orm import field
from spanner_orm import foreign_key_relationship
from spanner_orm import index
from spanner_orm import relationship

from google.api_core import datetime_helpers
from google.cloud import spanner
from google.cloud import spanner_v1
import immutabledict

T = TypeVar('T')


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

  def types(self) -> Dict[str, spanner_v1.Type]:
    """Returns parameter types to be used in the SQL query.

    Returns:
      Dictionary mapping from parameter name to the type of the value that
      should be substituted for that parameter in the SQL query
    """
    if not self.model_class:
      raise error.SpannerError('Condition must be bound before usage')
    return self._types()

  @abc.abstractmethod
  def _types(self) -> Dict[str, spanner_v1.Type]:
    raise NotImplementedError

  @abc.abstractmethod
  def _validate(self, model_class: Type[Any]) -> None:
    raise NotImplementedError


GuessableParamType = Union[
    bool,  #
    int,  #
    float,  #
    datetime_helpers.DatetimeWithNanoseconds,  #
    datetime.datetime,  #
    datetime.date,  #
    bytes,  #
    str,  #
    decimal.Decimal,  #
    # These types technically include List[None] and Tuple[None, ...], but
    # those can't be guessed.
    List[Optional[bool]],  #
    List[Optional[int]],  #
    List[Optional[float]],  #
    List[Optional[datetime_helpers.DatetimeWithNanoseconds]],  #
    List[Optional[datetime.datetime]],  #
    List[Optional[datetime.date]],  #
    List[Optional[bytes]],  #
    List[Optional[str]],  #
    List[Optional[decimal.Decimal]],  #
    Tuple[Optional[bool], ...],  #
    Tuple[Optional[int], ...],  #
    Tuple[Optional[float], ...],  #
    Tuple[Optional[datetime_helpers.DatetimeWithNanoseconds], ...],  #
    Tuple[Optional[datetime.datetime], ...],  #
    Tuple[Optional[datetime.date], ...],  #
    Tuple[Optional[bytes], ...],  #
    Tuple[Optional[str], ...],  #
    Tuple[Optional[decimal.Decimal], ...],  #
]


def _spanner_type_of_python_object(
    value: GuessableParamType) -> spanner_v1.Type:
  """Returns the Cloud Spanner type of the given object.

  Args:
    value: Object to guess the type of.
  """
  # See
  # https://github.com/googleapis/python-spanner/blob/master/google/cloud/spanner_v1/proto/type.proto
  # for the Cloud Spanner types, and
  # https://github.com/googleapis/python-spanner/blob/e981adb3157bb06e4cb466ca81d74d85da976754/google/cloud/spanner_v1/_helpers.py#L91-L133
  # for Python types.
  if value is None:
    raise TypeError(
        'Cannot infer type of None, because any SQL type can be NULL.')
  simple_type_code = {
      bool: spanner_v1.TypeCode.BOOL,
      int: spanner_v1.TypeCode.INT64,
      float: spanner_v1.TypeCode.FLOAT64,
      datetime_helpers.DatetimeWithNanoseconds: spanner_v1.TypeCode.TIMESTAMP,
      datetime.datetime: spanner_v1.TypeCode.TIMESTAMP,
      datetime.date: spanner_v1.TypeCode.DATE,
      bytes: spanner_v1.TypeCode.BYTES,
      str: spanner_v1.TypeCode.STRING,
      decimal.Decimal: spanner_v1.TypeCode.NUMERIC,
  }.get(type(value))
  if simple_type_code is not None:
    return spanner_v1.Type(code=simple_type_code)
  elif isinstance(value, (list, tuple)):
    element_types = tuple(
        _spanner_type_of_python_object(item)
        for item in value
        if item is not None)
    if element_types and all(
        a == b for a, b in zip(element_types, element_types[1:])):
      return spanner_v1.Type(
          code=spanner_v1.TypeCode.ARRAY,
          array_element_type=element_types[0],
      )
    else:
      raise TypeError(
          f'Array does not have elements of exactly one type: {value!r}')
  else:
    raise TypeError('Unknown type: {value!r}')


@dataclasses.dataclass
class Param:
  """Parameter for substitution into a SQL query."""
  value: Any
  type: spanner_v1.Type

  @classmethod
  def from_value(cls: Type[T], value: GuessableParamType) -> T:
    """Returns a Param with the type guessed from a Python value."""
    guessed_type = _spanner_type_of_python_object(value)

    # BYTES must be base64-encoded, see
    # https://github.com/googleapis/python-spanner/blob/87789c939990794bfd91f5300bedc449fd74bd7e/google/cloud/spanner_v1/proto/type.proto#L108-L110
    if (isinstance(value, bytes) and guessed_type == spanner.param_types.BYTES):
      encoded_value = base64.b64encode(value).decode()
    elif (isinstance(value, (list, tuple)) and
          all(isinstance(x, bytes) for x in value if x is not None) and
          guessed_type == spanner_v1.Type(
              code=spanner_v1.TypeCode.ARRAY,
              array_element_type=spanner.param_types.BYTES,
          )):
      encoded_value = tuple(
          None if item is None else base64.b64encode(item).decode()
          for item in value)
    else:
      encoded_value = value

    return cls(value=encoded_value, type=guessed_type)


@dataclasses.dataclass
class Column:
  """Named column; consider using field.Field instead."""
  name: str


# Something that can be substituted into a SQL query.
Substitution = Union[Param, field.Field, Column]


class ArbitraryCondition(Condition):
  """Condition with support for arbitrary SQL."""

  def __init__(
      self,
      sql_template: str,
      substitutions: Mapping[str, Substitution] = immutabledict.immutabledict(),
      *,
      segment: Segment,
  ):
    """Initializer.

    Args:
      sql_template: string.Template-compatible template string for the SQL.
      substitutions: Substitutions to make in sql_template.
      segment: Segment for this Condition.
    """
    super().__init__()
    self._sql_template = string.Template(sql_template)
    self._substitutions = substitutions
    self._segment = segment

    # This validates the template.
    self._sql_template.substitute({k: '' for k in self._substitutions})

  def segment(self) -> Segment:
    """See base class."""
    return self._segment

  def _validate(self, model_class: Type[Any]) -> None:
    """See base class."""
    for substitution in self._substitutions.values():
      if isinstance(substitution, field.Field):
        if substitution not in model_class.fields.values():
          raise error.ValidationError(
              f'Field {substitution.name!r} does not belong to the Model for '
              f'table {model_class.table!r}.')
      elif isinstance(substitution, Column):
        if substitution.name not in model_class.fields:
          raise error.ValidationError(
              f'Column {substitution.name!r} does not exist in the Model for '
              f'table {model_class.table!r}.')

  def _params(self) -> Dict[str, Any]:
    """See base class."""
    return {
        self.key(k): v.value
        for k, v in self._substitutions.items()
        if isinstance(v, Param)
    }

  def _types(self) -> Dict[str, spanner_v1.Type]:
    """See base class."""
    return {
        self.key(k): v.type
        for k, v in self._substitutions.items()
        if isinstance(v, Param)
    }

  def _sql_for_substitution(self, key: str, substitution: Substitution) -> str:
    if isinstance(substitution, Param):
      return f'@{self.key(key)}'
    else:
      assert isinstance(substitution, (field.Field, Column))
      return f'{self.model_class.column_prefix}.{substitution.name}'

  def _sql(self) -> str:
    """See base class."""
    return self._sql_template.substitute({
        k: self._sql_for_substitution(k, v)
        for k, v in self._substitutions.items()
    })


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

  def _types(self) -> Dict[str, spanner_v1.Type]:
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

    if (not origin.field_type().comparable_with(dest.field_type()) or
        origin.nullable() != dest.nullable()):
      raise error.ValidationError('Types of {} and {} do not match'.format(
          origin.name, dest.name))


class _IndexCondition(Condition):
  """Base class for conditions based on an Index."""

  def __init__(self, index_or_name: Union[index.Index, str]):
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

  def _validate(self, model_class: Type[Any]) -> None:
    if self.name not in model_class.indexes:
      raise error.ValidationError('{} is not an index on {}'.format(
          self.name, model_class.table))
    if self.index and self.index != model_class.indexes[self.name]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.index.name, model_class.table))


class ForceIndexCondition(_IndexCondition):
  """Used to indicate which index should be used in a Spanner query."""

  def __init__(
      self,
      index_or_name: Union[index.Index, str],
      *,
      extra_hints: Sequence[str] = (),
  ):
    super().__init__(index_or_name)
    self._extra_hints = extra_hints

  def _params(self) -> Dict[str, Any]:
    return {}

  def segment(self) -> Segment:
    return Segment.FROM

  def _sql(self) -> str:
    hints = (f'FORCE_INDEX={self.name}', *self._extra_hints)
    return f'@{{{",".join(hints)}}}'

  def _types(self) -> Dict[str, spanner_v1.Type]:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    super()._validate(model_class)
    if model_class.indexes[self.name].primary:
      raise error.ValidationError('Cannot force query using primary index')


class _IndexIgnoreNullsCondition(_IndexCondition):
  """Condition to filter NULL values in any column of an index."""

  def _params(self) -> Dict[str, Any]:
    return {}

  def segment(self) -> Segment:
    return Segment.WHERE

  def _sql(self) -> str:
    return '({})'.format(' AND '.join(
        f'{column} IS NOT NULL' for column in self.index.columns))

  def _types(self) -> Dict[str, spanner_v1.Type]:
    return {}


class IncludesCondition(Condition):
  """Used to include related model_classs via a relation in a Spanner query."""

  def __init__(
      self,
      relation_or_name: Union[relationship.Relationship,
                              foreign_key_relationship.ForeignKeyRelationship,
                              str],
      conditions: Optional[List[Condition]] = None,
      # Default argument is `False` for backwards-compatability.
      foreign_key_relation=False,
  ):
    """Initializer.


    Args:
      relation: Name of the relationship on the origin model or the Relationship/
        ForeignKeyRelationship on the origin model class used to retrieve
        associated objects
      conditions: Conditions to apply on the subquery
      foreign_key_relation: True if the relation is a foreign key relation,
       False if it is a legacy relation (eg not enforced in Spanner)
    """
    super().__init__()
    self.foreign_key_relation = foreign_key_relation
    if isinstance(relation_or_name, relationship.Relationship):
      if foreign_key_relation:
        raise ValueError('Must pass foreign key relation if '
                         '`foreign_key_relation=True`.')
      self.name = relation_or_name.name
      self.relation = relation_or_name
    elif isinstance(relation_or_name,
                    foreign_key_relationship.ForeignKeyRelationship):
      if not foreign_key_relation:
        raise ValueError(
            'Must pass legacy relation if `foreign_key_relation=False`.')
      self.name = relation_or_name.name
      self.relation = relation_or_name
    else:
      self.name = relation_or_name
      self.relation = None
    self._conditions = conditions or []

  def bind(self, model_class: Type[Any]) -> None:
    super().bind(model_class)
    if self.foreign_key_relation:
      self.relation = self.model_class.foreign_key_relations[self.name]
    else:
      self.relation = self.model_class.relations[self.name]

  @property
  def conditions(self) -> List[Condition]:
    """Generate the child conditions based on the relationship constraints."""
    relation_conditions = []
    if isinstance(self.relation,
                  foreign_key_relationship.ForeignKeyRelationship):
      for pair in self.relation.constraint.columns.items():
        referencing_column, referenced_column = pair
        relation_conditions.append(
            ColumnsEqualCondition(referenced_column, self.model_class,
                                  referencing_column))
    elif isinstance(self.relation, relationship.Relationship):
      for constraint in self.relation.constraints:
        # This is backward from what you might imagine because the condition
        # will be processed from the context of the destination model.
        relation_conditions.append(
            ColumnsEqualCondition(constraint.destination_column,
                                  constraint.origin_class,
                                  constraint.origin_column))
    else:
      raise error.SpannerError(
          'Condition must be bound before conditions is called')
    return relation_conditions + self._conditions

  @property
  def destination(self) -> Type[Any]:
    if isinstance(self.relation,
                  foreign_key_relationship.ForeignKeyRelationship):
      return self.relation.constraint.referenced_table
    elif isinstance(self.relation, relationship.Relationship):
      return self.relation.destination
    else:
      raise error.SpannerError(
          'Condition must be bound before destination is called')

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

  def _types(self) -> Dict[str, spanner_v1.Type]:
    return {}

  def _validate(self, model_class: Type[Any]) -> None:
    if self.foreign_key_relation:
      model_class_relations = model_class.foreign_key_relations
      referenced_table_fn = lambda x: x.constraint.referenced_table
    else:
      model_class_relations = model_class.relations
      referenced_table_fn = lambda x: x.destination

    if self.name not in model_class_relations:
      raise error.ValidationError('{} is not a relation on {}'.format(
          self.name, model_class.table))
    if self.relation and self.relation != model_class_relations[self.name]:
      raise error.ValidationError('{} does not belong to {}'.format(
          self.relation.name, model_class.table))
    other_model_class = referenced_table_fn(model_class_relations[self.name])

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

  def _types(self) -> Dict[str, spanner_v1.Type]:
    types = {self._limit_key: spanner.param_types.INT64}
    if self.offset:
      types[self._offset_key] = spanner.param_types.INT64
    return types

  def _validate(self, model_class: Type[Any]) -> None:
    # Validation is independent of model_class for LIMIT
    del model_class


class OrCondition(Condition):
  """Used to join multiple conditions with an OR in a Spanner query."""

  def __init__(self, *condition_lists: List[Condition]):
    super().__init__()
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
      if conditions:
        new_segment = ' AND '.join(
            [condition.sql() for condition in conditions])
        segments.append('({new_segment})'.format(new_segment=new_segment))
      else:
        segments.append('TRUE')
    if segments:
      return '({segments})'.format(segments=' OR '.join(segments))
    else:
      return 'FALSE'

  def segment(self) -> Segment:
    return Segment.WHERE

  def _types(self) -> spanner_v1.Type:
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

  def _types(self) -> spanner_v1.Type:
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

  def _types(self) -> spanner_v1.Type:
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

  def _types(self) -> spanner_v1.Type:
    grpc_type = self.model_class.fields[self.column].grpc_type()
    list_type = spanner_v1.Type(
        code=spanner_v1.TypeCode.ARRAY, array_element_type=grpc_type)
    return {self._column_key: list_type}

  def _validate(self, model_class: Type[Any]) -> None:
    if not isinstance(self.value, Iterable):
      raise error.ValidationError('{} is not iterable'.format(self.value))
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

  def _types(self) -> spanner_v1.Type:
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


def contains(
    haystack: Substitution,
    needle: Substitution,
    *,
    case_sensitive: bool = True,
) -> Condition:
  """Condition where the specified haystack contains the given needle.

  Args:
    haystack: String or bytes to search.
    needle: String or bytes to search for. Must be the same type as haystack.
    case_sensitive: Whether comparison should be case sensitive or not. See
      https://cloud.google.com/spanner/docs/functions-and-operators#lower for
      caveats on how the case conversion works.

  Returns:
    A Condition subclass that will be used in the query
  """
  return ArbitraryCondition(
      ('STRPOS($haystack, $needle) > 0'
       if case_sensitive else 'STRPOS(LOWER($haystack), LOWER($needle)) > 0'),
      dict(
          haystack=haystack,
          needle=needle,
      ),
      segment=Segment.WHERE,
  )


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


def force_null_filtered_index(
    forced_index: Union[index.Index, str]) -> Sequence[Condition]:
  """Returns conditions to force the query to use the given NULL_FILTERED index.

  In Cloud Spanner, a query against a NULL_FILTERED index is tested to see if it
  can use safely use that index. If using the index would result in incorrect
  results (e.g., by ignoring NULL values that would be in the same query without
  using the index), it's an error. However, the Cloud Spanner Emulator
  doesn't support that check:
  https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/e887ff5569684e6e45ce7c90d0fdfb7b1faa1491/common/errors.cc#L1790-L1800

  For queries that can safely ignore any  NULL values covered by the index, this
  function returns conditions that both filter out all relevant NULLs (avoiding
  the potential error in Cloud Spanner) and disable the check in Cloud Spanner
  Emulator.

  Args:
    forced_index: NULL_FILTERED index to use.
  """
  return (
      ForceIndexCondition(
          forced_index,
          extra_hints=(
              'spanner_emulator.disable_query_null_filtered_index_check=true',
          )),
      _IndexIgnoreNullsCondition(forced_index),
  )


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


def includes(relation: Union[relationship.Relationship,
                             foreign_key_relationship.ForeignKeyRelationship,
                             str],
             conditions: Optional[List[Condition]] = None,
             foreign_key_relation: bool = False) -> IncludesCondition:
  """Condition where the objects associated with a relationship are retrieved.

  Note that the query formed by this call is not a JOIN, but instead a
  subquery on the table on the other side of the relationship. To narrow
  down the objects retrieved in the subquery, conditions that apply to that
  subquery may be included, but not all conditions may apply

  Args:
    relation: Name of the relationship on the origin model or the Relationship/
      ForeignKeyRelationship on the origin model class used to retrieve
      associated objects
    conditions: Conditions to apply on the subquery
    foreign_key_relation: True if the relation is a foreign key relation,
      False if it is a legacy relation (ie not enforced in Spanner)

  Returns:
    A Condition subclass that will be used in the query
  """
  return IncludesCondition(relation, conditions, foreign_key_relation)


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

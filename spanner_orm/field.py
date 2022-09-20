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
"""Helper to deal with field types in Spanner interactions."""

import abc
import base64
import binascii
import datetime
from typing import Any, Optional, Type, Union
import warnings

# TODO(https://github.com/google/pytype/issues/1081): Re-enable import-error.
from google.cloud import spanner  # pytype: disable=import-error
from google.cloud import spanner_v1  # pytype: disable=import-error
from spanner_orm import error


class FieldType(abc.ABC):
  """Base class for column types for Spanner interactions."""

  @abc.abstractmethod
  def ddl(self) -> str:
    """Returns the DDL for this type."""
    raise NotImplementedError

  @abc.abstractmethod
  def grpc_type(self) -> spanner_v1.Type:
    """Returns the type as used in Cloud Spanner's gRPC API."""
    raise NotImplementedError

  @abc.abstractmethod
  def validate_type(self, value: Any) -> None:
    """Raises error.ValidationError if value doesn't match the type."""
    raise NotImplementedError

  def comparable_with(self, other: 'FieldType') -> bool:
    """Returns whether two types are comparable."""
    # https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#comparable_data_types
    return type(self) == type(other)


class Field:
  """Represents a column in a table as a field in a model.

  Attributes:
    name: Name of the column, or None if this hasn't been bound to a column yet.
  """
  name: Optional[str]

  def __init__(
      self,
      field_type: Union[FieldType, Type[FieldType]],
      *,
      nullable: bool = False,
      primary_key: bool = False,
  ):
    """Initializer.

    Args:
      field_type: Type of the field. Passing a class instead of an instance of
        that class is deprecated.
      nullable: Whether the field can be NULL.
      primary_key: Whether the field is part of the table's primary key.
    """
    self.name = None
    if isinstance(field_type, FieldType):
      self._type = field_type
    else:
      warnings.warn(
          DeprecationWarning(
              'Pass an instance of FieldType instead of a class.'))
      self._type = field_type()
    self._nullable = nullable
    self._primary_key = primary_key

  def ddl(self) -> str:
    """Returns DDL for the column."""
    if self._nullable:
      return self._type.ddl()
    return f'{self._type.ddl()} NOT NULL'

  def field_type(self) -> FieldType:
    """Returns the type of the field."""
    return self._type

  def grpc_type(self) -> str:
    """Returns the type as used in Cloud Spanner's gRPC API."""
    return self._type.grpc_type()

  def nullable(self) -> bool:
    """Returns whether the field can be NULL."""
    return self._nullable

  def primary_key(self) -> bool:
    """Returns whether the field is part of the table's primary key."""
    return self._primary_key

  def validate(self, value: Any) -> None:
    """Raises error.ValidationError if value isn't compatible with the field."""
    if value is None:
      if not self._nullable:
        raise error.ValidationError('None set for non-nullable field')
    else:
      self._type.validate_type(value)


class Boolean(FieldType):
  """Represents a boolean type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'BOOL'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.BOOL

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, bool):
      raise error.ValidationError(f'{value!r} is not of type bool')


class Integer(FieldType):
  """Represents an integer type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'INT64'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.INT64

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, int):
      raise error.ValidationError(f'{value!r} is not of type int')


class Float(FieldType):
  """Represents a float type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'FLOAT64'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.FLOAT64

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, (int, float)):
      raise error.ValidationError(f'{value!r} is not of type float')


class String(FieldType):
  """Represents a string type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'STRING(MAX)'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.STRING

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, str):
      raise error.ValidationError(f'{value!r} is not of type str')


class StringArray(FieldType):
  """Represents an array of strings type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'ARRAY<STRING(MAX)>'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.Array(spanner.param_types.STRING)

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, list):
      raise error.ValidationError(f'{value!r} is not of type list')
    for item in value:
      if not isinstance(item, str):
        raise error.ValidationError(f'{item!r} is not of type str')


class Timestamp(FieldType):
  """Represents a timestamp type."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'TIMESTAMP'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.TIMESTAMP

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, datetime.datetime):
      raise error.ValidationError(f'{value!r} is not of type datetime')


class BytesBase64(FieldType):
  """Represents a bytes type that must be base64 encoded."""

  def ddl(self) -> str:
    """See base class."""
    del self  # Unused.
    return 'BYTES(MAX)'

  def grpc_type(self) -> spanner_v1.Type:
    """See base class."""
    del self  # Unused.
    return spanner.param_types.BYTES

  def validate_type(self, value: Any) -> None:
    """See base class."""
    del self  # Unused.
    if not isinstance(value, bytes):
      raise error.ValidationError(f'{value!r} is not of type bytes')
    # Rudimentary test to check for base64 encoding.
    try:
      base64.b64decode(value, altchars=None, validate=True)
    except binascii.Error:
      raise error.ValidationError(f'{value!r} must be base64-encoded bytes.')


def field_type_from_ddl(ddl: str) -> FieldType:
  """Returns the the field type for the given DDL expression."""
  if ddl == 'BOOL':
    return Boolean()
  elif ddl == 'INT64':
    return Integer()
  elif ddl == 'FLOAT64':
    return Float()
  elif ddl == 'STRING(MAX)':
    return String()
  elif ddl == 'ARRAY<STRING(MAX)>':
    return StringArray()
  elif ddl == 'TIMESTAMP':
    return Timestamp()
  elif ddl == 'BYTES(MAX)':
    return BytesBase64()
  else:
    raise error.SpannerError(f'Invalid or unimplemented DDL type: {ddl!r}')

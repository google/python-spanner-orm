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
from typing import Any, Optional, Type

# TODO(https://github.com/google/pytype/issues/1081): Re-enable import-error.
from google.cloud import spanner  # pytype: disable=import-error
from google.cloud import spanner_v1  # pytype: disable=import-error
from spanner_orm import error


class FieldType(abc.ABC):
  """Base class for column types for Spanner interactions."""

  @staticmethod
  @abc.abstractmethod
  def ddl() -> str:
    """Returns the DDL for this type."""
    raise NotImplementedError

  @staticmethod
  @abc.abstractmethod
  def grpc_type() -> spanner_v1.Type:
    """Returns the type as used in Cloud Spanner's gRPC API."""
    raise NotImplementedError

  @staticmethod
  @abc.abstractmethod
  def validate_type(value: Any) -> None:
    """Raises error.ValidationError if value doesn't match the type."""
    raise NotImplementedError


class Field:
  """Represents a column in a table as a field in a model.

  Attributes:
    name: Name of the column, or None if this hasn't been bound to a column yet.
  """
  name: Optional[str]

  def __init__(
      self,
      field_type: Type[FieldType],
      *,
      nullable: bool = False,
      primary_key: bool = False,
  ):
    """Initializer.

    Args:
      field_type: Type of the field.
      nullable: Whether the field can be NULL.
      primary_key: Whether the field is part of the table's primary key.
    """
    self.name = None
    self._type = field_type
    self._nullable = nullable
    self._primary_key = primary_key

  def ddl(self) -> str:
    """Returns DDL for the column."""
    if self._nullable:
      return self._type.ddl()
    return f'{self._type.ddl()} NOT NULL'

  def field_type(self) -> Type[FieldType]:
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

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'BOOL'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.BOOL

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, bool):
      raise error.ValidationError(f'{value!r} is not of type bool')


class Integer(FieldType):
  """Represents an integer type."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'INT64'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.INT64

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, int):
      raise error.ValidationError(f'{value!r} is not of type int')


class Float(FieldType):
  """Represents a float type."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'FLOAT64'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.FLOAT64

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, (int, float)):
      raise error.ValidationError(f'{value!r} is not of type float')


class String(FieldType):
  """Represents a string type."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'STRING(MAX)'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.STRING

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, str):
      raise error.ValidationError(f'{value!r} is not of type str')


class StringArray(FieldType):
  """Represents an array of strings type."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'ARRAY<STRING(MAX)>'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.Array(spanner.param_types.STRING)

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, list):
      raise error.ValidationError(f'{value!r} is not of type list')
    for item in value:
      if not isinstance(item, str):
        raise error.ValidationError(f'{item!r} is not of type str')


class Timestamp(FieldType):
  """Represents a timestamp type."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'TIMESTAMP'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.TIMESTAMP

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, datetime.datetime):
      raise error.ValidationError(f'{value!r} is not of type datetime')


class BytesBase64(FieldType):
  """Represents a bytes type that must be base64 encoded."""

  @staticmethod
  def ddl() -> str:
    """See base class."""
    return 'BYTES(MAX)'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    """See base class."""
    return spanner.param_types.BYTES

  @staticmethod
  def validate_type(value: Any) -> None:
    """See base class."""
    if not isinstance(value, bytes):
      raise error.ValidationError(f'{value!r} is not of type bytes')
    # Rudimentary test to check for base64 encoding.
    try:
      base64.b64decode(value, altchars=None, validate=True)
    except binascii.Error:
      raise error.ValidationError(f'{value!r} must be base64-encoded bytes.')


ALL_TYPES = (
    Boolean,
    Integer,
    Float,
    String,
    StringArray,
    Timestamp,
    BytesBase64,
)

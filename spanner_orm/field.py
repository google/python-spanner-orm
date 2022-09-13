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
"""Helper to deal with field types in Spanner interactions."""

import abc
import base64
import binascii
import datetime
from typing import Any, Type

from google.cloud import spanner
from google.cloud import spanner_v1
from spanner_orm import error


class FieldType(abc.ABC):
  """Base class for column types for Spanner interactions."""

  @staticmethod
  @abc.abstractmethod
  def ddl() -> str:
    raise NotImplementedError

  @staticmethod
  @abc.abstractmethod
  def grpc_type() -> spanner_v1.Type:
    raise NotImplementedError

  @staticmethod
  @abc.abstractmethod
  def validate_type(value: Any) -> None:
    raise NotImplementedError


class Field(object):
  """Represents a column in a table as a field in a model."""

  def __init__(self,
               field_type: Type[FieldType],
               nullable: bool = False,
               primary_key: bool = False,
               length: int = 0):
    self.name = None
    self._type = field_type
    self._nullable = nullable
    self._primary_key = primary_key
    self._length = length

    if self._length < 0:
      raise error.ValidationError('length can not be less than zero')

    length_support = ["STRING(MAX)", "ARRAY<STRING(MAX)>", "BYTES(MAX)"]

    if self._type.ddl() not in length_support and self._length:
      raise error.ValidationError('length can not be set on field {}'.format(
          self._type))

  def ddl(self) -> str:
    base_ddl = self._type.ddl()
    if self._length:
      base_ddl = base_ddl.replace('(MAX)', f'({self._length})')
    if self._nullable:
      return base_ddl
    return '{field_type} NOT NULL'.format(field_type=base_ddl)

  def field_type(self) -> Type[FieldType]:
    return self._type

  def grpc_type(self) -> str:
    return self._type.grpc_type()

  def nullable(self) -> bool:
    return self._nullable

  def primary_key(self) -> bool:
    return self._primary_key

  def validate(self, value) -> None:
    if value is None:
      if not self._nullable:
        raise error.ValidationError('None set for non-nullable field')
    else:
      self._type.validate_type(value)


class Boolean(FieldType):
  """Represents a boolean type."""

  @staticmethod
  def ddl() -> str:
    return 'BOOL'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.BOOL

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, bool):
      raise error.ValidationError('{} is not of type bool'.format(value))


class Integer(FieldType):
  """Represents an integer type."""

  @staticmethod
  def ddl() -> str:
    return 'INT64'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.INT64

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, int):
      raise error.ValidationError('{} is not of type int'.format(value))


class Float(FieldType):
  """Represents a float type."""

  @staticmethod
  def ddl() -> str:
    return 'FLOAT64'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.FLOAT64

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, (int, float)):
      raise error.ValidationError('{} is not of type float'.format(value))


class String(FieldType):
  """Represents a string type."""

  @staticmethod
  def ddl() -> str:
    return 'STRING(MAX)'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.STRING

  @staticmethod
  def validate_type(value) -> None:
    if not isinstance(value, str):
      raise error.ValidationError('{} is not of type str'.format(value))


class StringArray(FieldType):
  """Represents an array of strings type."""

  @staticmethod
  def ddl() -> str:
    return 'ARRAY<STRING(MAX)>'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.Array(spanner.param_types.STRING)

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, list):
      raise error.ValidationError('{} is not of type list'.format(value))
    for item in value:
      if not isinstance(item, str):
        raise error.ValidationError('{} is not of type str'.format(item))


class IntArray(FieldType):
  """Represents an array of int64 type."""

  @staticmethod
  def ddl() -> str:
    return 'ARRAY<INT64>'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.Array(spanner.param_types.INT64)

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, list):
      raise error.ValidationError('{} is not of type list'.format(value))
    for item in value:
      if not isinstance(item, int):
        raise error.ValidationError('{} is not of type int'.format(item))


class Timestamp(FieldType):
  """Represents a timestamp type."""

  @staticmethod
  def ddl() -> str:
    return 'TIMESTAMP'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.TIMESTAMP

  @staticmethod
  def validate_type(value: Any) -> None:
    if not isinstance(value, datetime.datetime):
      raise error.ValidationError('{} is not of type datetime'.format(value))


class BytesBase64(FieldType):
  """Represents a bytes type that must be base64 encoded."""

  @staticmethod
  def ddl() -> str:
    return 'BYTES(MAX)'

  @staticmethod
  def grpc_type() -> spanner_v1.Type:
    return spanner.param_types.BYTES

  @staticmethod
  def validate_type(value) -> None:
    if not isinstance(value, bytes):
      raise error.ValidationError('{} is not of type bytes'.format(value))
    # Rudimentary test to check for base64 encoding.
    try:
      base64.b64decode(value, altchars=None, validate=True)
    except binascii.Error:
      raise error.ValidationError(
          '{} must be base64-encoded bytes.'.format(value))


ALL_TYPES = [
    Boolean,
    Integer,
    IntArray,
    Float,
    String,
    StringArray,
    Timestamp,
    BytesBase64,
]

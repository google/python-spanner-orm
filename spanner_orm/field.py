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
"""Helper to deal with field types in Spanner interactions."""

import abc
import datetime

from google.cloud.spanner_v1.proto import type_pb2


class Field(object):
  """Represents a column in a table as a field in a model."""

  def __init__(self, field_type, nullable=False):
    self._type = field_type
    self._nullable = nullable

  def ddl(self):
    if self._nullable:
      return self._type.ddl()
    return '{field_type} NOT NULL'.format(field_type=self._type.ddl())

  def field_type(self):
    return self._type

  def grpc_type(self):
    return self._type.grpc_type()

  def grpc_list_type(self):
    return self._type.grpc_list_type()

  def nullable(self):
    return self._nullable

  def validate(self, value):
    if value is None:
      assert self._nullable
    else:
      self._type.validate_type(value)


class FieldType(abc.ABC):
  """Base class for column types for Spanner interactions."""

  @staticmethod
  @abc.abstractmethod
  def ddl():
    raise NotImplementedError

  @staticmethod
  @abc.abstractmethod
  def grpc_type():
    raise NotImplementedError

  @classmethod
  def grpc_list_type(cls):
    return type_pb2.Type(
        code=type_pb2.ARRAY, array_element_type=cls.grpc_type())

  @staticmethod
  @abc.abstractmethod
  def validate_type(value):
    raise NotImplementedError


class Boolean(FieldType):
  """Represents a boolean type."""

  @staticmethod
  def ddl():
    return 'BOOL'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.BOOL)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, bool), '{} is not of type bool'.format(value)


class Integer(FieldType):
  """Represents an integer type."""

  @staticmethod
  def ddl():
    return 'INT64'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.INT64)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, int), '{} is not of type int'.format(value)


class String(FieldType):
  """Represents a string type."""

  @staticmethod
  def ddl():
    return 'STRING(MAX)'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.STRING)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, str), '{} is not of type str'.format(value)


class StringArray(FieldType):
  """Represents an array of strings type."""

  @staticmethod
  def ddl():
    return 'ARRAY<STRING(MAX)>'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.ARRAY)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, list), '{} is not of type list'.format(value)
    for item in value:
      assert isinstance(item, str), '{} is not of type str'.format(item)


class Timestamp(FieldType):
  """Represents a timestamp type."""

  @staticmethod
  def ddl():
    return 'TIMESTAMP'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.TIMESTAMP)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, datetime.datetime)


ALL_TYPES = [Boolean, Integer, String, StringArray, Timestamp]

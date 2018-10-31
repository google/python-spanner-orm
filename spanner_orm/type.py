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
"""Helper to deal with column types in database interactions"""

from abc import ABC
from abc import abstractmethod
from datetime import datetime

from google.cloud.spanner_v1.proto import type_pb2


class DatabaseType(ABC):
  """Base class for column types for database interactions"""

  @classmethod
  def full_ddl(cls):
    if issubclass(cls, NullableType):
      return cls.ddl()
    else:
      return '{} NOT NULL'.format(cls.ddl())

  @staticmethod
  @abstractmethod
  def db_type():
    raise NotImplementedError

  @staticmethod
  @abstractmethod
  def ddl():
    raise NotImplementedError

  @staticmethod
  @abstractmethod
  def grpc_type():
    raise NotImplementedError

  @classmethod
  def grpc_list_type(cls):
    return type_pb2.Type(
        code=type_pb2.ARRAY, array_element_type=cls.grpc_type())

  @classmethod
  def validate(cls, value):
    if value is None:
      assert issubclass(cls, NullableType), 'Null value for non-nullable column'
    else:
      cls.validate_type(value)

  @staticmethod
  @abstractmethod
  def validate_type(value):
    raise NotImplementedError


class NullableType(ABC):
  pass


class Boolean(DatabaseType):
  """Represents an integer type"""

  @staticmethod
  def db_type():
    return Boolean

  @staticmethod
  def ddl():
    return 'BOOL'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.BOOL)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, bool), '{} is not of type bool'.format(value)


class NullableBoolean(NullableType, Boolean):
  pass


class Integer(DatabaseType):
  """Represents an integer type"""

  @staticmethod
  def db_type():
    return Integer

  @staticmethod
  def ddl():
    return 'INT64'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.INT64)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, int), '{} is not of type int'.format(value)


class NullableInteger(NullableType, Integer):
  pass


class String(DatabaseType):
  """Represents a string type"""

  @staticmethod
  def db_type():
    return String

  @staticmethod
  def ddl():
    return 'STRING(MAX)'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.STRING)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, str), '{} is not of type str'.format(value)


class NullableString(NullableType, String):
  pass


class StringArray(DatabaseType):
  """Represents an array of strings type"""

  @staticmethod
  def db_type():
    return StringArray

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


class NullableStringArray(NullableType, StringArray):
  pass


class Timestamp(DatabaseType):
  """Represents a timestamp type"""

  @staticmethod
  def db_type():
    return Timestamp

  @staticmethod
  def ddl():
    return 'TIMESTAMP'

  @staticmethod
  def grpc_type():
    return type_pb2.Type(code=type_pb2.TIMESTAMP)

  @staticmethod
  def validate_type(value):
    assert isinstance(value, datetime)


class NullableTimestamp(NullableType, Timestamp):
  pass


ALL_TYPES = [
    Boolean, NullableBoolean, Integer, NullableInteger, String, NullableString,
    StringArray, NullableStringArray, Timestamp, NullableTimestamp
]

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
"""Models used by unit tests."""

from spanner_orm import model
from spanner_orm.relationship import ModelRelationship
from spanner_orm.type import Integer
from spanner_orm.type import NullableInteger
from spanner_orm.type import NullableString
from spanner_orm.type import NullableStringArray
from spanner_orm.type import String
from spanner_orm.type import Timestamp


class ChildTestModel(model.Model):
  """Model class for testing relationships"""

  @staticmethod
  def primary_index_keys():
    return ['parent_key', 'child_key']

  @classmethod
  def relations(cls):
    return {
        'parent':
            ModelRelationship(cls, 'spanner_orm.tests.models.SmallTestModel',
                              {'parent_key': 'key'})
    }

  @classmethod
  def schema(cls):
    return {'parent_key': String, 'child_key': String}

  @classmethod
  def table(cls):
    return 'ChildTestModel'


class SmallTestModel(model.Model):
  """Model class used for testing"""

  @staticmethod
  def primary_index_keys():
    return ['key']

  @classmethod
  def schema(cls):
    return {'key': String, 'value_1': String, 'value_2': NullableString}

  @classmethod
  def table(cls):
    return 'SmallTestModel'


class UnittestModel(model.Model):
  """Model class used for model testing"""

  @staticmethod
  def primary_index_keys():
    return ['int_', 'string']

  @staticmethod
  def schema():
    return {
        'int_': Integer,
        'int_2': NullableInteger,
        'string': String,
        'string_2': NullableString,
        'timestamp': Timestamp,
        'string_array': NullableStringArray
    }

  @classmethod
  def table(cls):
    return 'table'

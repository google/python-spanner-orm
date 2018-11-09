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

from spanner_orm import field
from spanner_orm import model
from spanner_orm import relationship


class ChildTestModel(model.Model):
  """Model class for testing relationships"""

  @classmethod
  def relations(cls):
    return {
        'parent':
            relationship.ModelRelationship(
                cls, 'spanner_orm.tests.models.SmallTestModel',
                {'parent_key': 'key'})
    }

  @staticmethod
  def primary_index_keys():
    return ['parent_key', 'child_key']

  __table__ = 'ChildTestModel'
  parent_key = field.Field(field.String)
  child_key = field.Field(field.String)


class SmallTestModel(model.Model):
  """Model class used for testing"""

  @staticmethod
  def primary_index_keys():
    return ['key']

  __table__ = 'SmallTestModel'
  key = field.Field(field.String)
  value_1 = field.Field(field.String)
  value_2 = field.Field(field.String, nullable=True)


class UnittestModel(model.Model):
  """Model class used for model testing"""

  @staticmethod
  def primary_index_keys():
    return ['int_', 'string']

  __table__ = 'table'
  int_ = field.Field(field.Integer)
  int_2 = field.Field(field.Integer, nullable=True)
  string = field.Field(field.String)
  string_2 = field.Field(field.String, nullable=True)
  timestamp = field.Field(field.Timestamp)
  string_array = field.Field(field.StringArray, nullable=True)

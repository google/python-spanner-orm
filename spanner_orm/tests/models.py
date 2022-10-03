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
"""Models used by unit tests."""

from spanner_orm import field
from spanner_orm import foreign_key_relationship
from spanner_orm import index
from spanner_orm import model
from spanner_orm import relationship


class SmallTestModel(model.Model):
  """Model class used for testing."""

  __table__ = 'SmallTestModel'
  key = field.Field(field.String, primary_key=True)
  value_1 = field.Field(field.String)
  value_2 = field.Field(field.String, nullable=True)
  index_1 = index.Index(['value_1'])


class SmallTestModelWithoutSecondaryIndexes(model.Model):
  """Same as SmallTestModel, but with no secondary indexes."""
  __table__ = 'SmallTestModel'
  key = field.Field(field.String, primary_key=True)
  value_1 = field.Field(field.String)
  value_2 = field.Field(field.String, nullable=True)


class ChildTestModel(model.Model):
  """Model class for testing interleaved tables."""

  __table__ = 'ChildTestModel'
  __interleaved__ = 'SmallTestModel'

  key = field.Field(field.String, primary_key=True)
  child_key = field.Field(field.String, primary_key=True)


class IndexTestModel(model.Model):
  __table__ = 'IndexTestModel'

  key = field.Field(field.String, primary_key=True)
  value = field.Field(field.String)

  value_index = index.Index(['value'])


class RelationshipTestModel(model.Model):
  """Model class for testing relationships."""

  __table__ = 'RelationshipTestModel'
  parent_key = field.Field(field.String, primary_key=True)
  child_key = field.Field(field.String, primary_key=True)
  parent = relationship.Relationship(
      'spanner_orm.tests.models.SmallTestModel', {'parent_key': 'key'},
      single=True)
  parents = relationship.Relationship('spanner_orm.tests.models.SmallTestModel',
                                      {'parent_key': 'key'})


class ForeignKeyTestModel(model.Model):
  """Model class for testing foreign keys."""

  __table__ = 'ForeignKeyTestModel'
  referencing_key_1 = field.Field(field.String, primary_key=True)
  referencing_key_2 = field.Field(field.String, primary_key=True)
  referencing_key_3 = field.Field(field.Integer, primary_key=True)
  self_referencing_key = field.Field(field.String, nullable=True)
  foreign_key_1 = foreign_key_relationship.ForeignKeyRelationship(
      'spanner_orm.tests.models.SmallTestModel', {'referencing_key_1': 'key'})
  foreign_key_2 = foreign_key_relationship.ForeignKeyRelationship(
      'spanner_orm.tests.models.UnittestModel',
      {
          'referencing_key_2': 'string',
          'referencing_key_3': 'int_'
      },
  )
  foreign_key_3 = foreign_key_relationship.ForeignKeyRelationship(
      'spanner_orm.tests.models.ForeignKeyTestModel',
      {'self_referencing_key': 'referencing_key_1'})


class InheritanceTestModel(SmallTestModel):
  """Model class used for testing model inheritance."""
  value_3 = field.Field(field.String, nullable=True)


class UnittestModel(model.Model):
  """Model class used for model testing."""

  __table__ = 'table'
  int_ = field.Field(field.Integer, primary_key=True)
  int_2 = field.Field(field.Integer, nullable=True)
  float_ = field.Field(field.Float, primary_key=True)
  float_2 = field.Field(field.Float, nullable=True)
  string = field.Field(field.String, primary_key=True)
  string_2 = field.Field(field.String, nullable=True)
  string_3 = field.Field(field.String(20), nullable=True)
  bytes_ = field.Field(field.BytesBase64, primary_key=True)
  bytes_2 = field.Field(field.BytesBase64, nullable=True)
  bytes_3 = field.Field(field.BytesBase64(20), nullable=True)
  timestamp = field.Field(field.Timestamp)
  string_array = field.Field(field.StringArray, nullable=True)
  string_array_2 = field.Field(field.Array(field.String(20)), nullable=True)

  test_index = index.Index(['string_2'])


class UnittestModelWithoutSecondaryIndexes(model.Model):
  """Same as UnittestModel, but with no secondary indexes."""

  __table__ = 'table'
  int_ = field.Field(field.Integer, primary_key=True)
  int_2 = field.Field(field.Integer, nullable=True)
  float_ = field.Field(field.Float, primary_key=True)
  float_2 = field.Field(field.Float, nullable=True)
  string = field.Field(field.String, primary_key=True)
  string_2 = field.Field(field.String, nullable=True)
  string_3 = field.Field(field.String(20), nullable=True)
  bytes_ = field.Field(field.BytesBase64, primary_key=True)
  bytes_2 = field.Field(field.BytesBase64, nullable=True)
  bytes_3 = field.Field(field.BytesBase64(20), nullable=True)
  timestamp = field.Field(field.Timestamp)
  string_array = field.Field(field.StringArray, nullable=True)
  string_array_2 = field.Field(field.Array(field.String(20)), nullable=True)


class NullFilteredIndexModel(model.Model):
  """Model class for testing NULL_FILTERED indexes."""

  __table__ = 'NullFilteredIndexModel'
  key = field.Field(field.String, primary_key=True)
  value_1 = field.Field(field.String, nullable=True)
  value_2 = field.Field(field.Integer)
  value_index = index.Index(['value_1', 'value_2'], null_filtered=True)

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
import logging
import unittest
from unittest import mock

from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import update
from spanner_orm.tests import models


class UpdateTest(unittest.TestCase):

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_add_column(self, get_model):
    table_name = models.SmallTestModel.table
    get_model.return_value = models.SmallTestModel

    new_field = field.Field(field.String, nullable=True)
    test_update = update.AddColumn(table_name, 'bar', new_field)
    test_update.validate()
    self.assertEqual(
        test_update.ddl(),
        'ALTER TABLE {} ADD COLUMN bar STRING(MAX)'.format(table_name))

  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.count')
  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_drop_column(self, get_model, index_count):
    table_name = models.SmallTestModel.table
    get_model.return_value = models.SmallTestModel
    index_count.return_value = 0

    test_update = update.DropColumn(table_name, 'value_2')
    test_update.validate()
    self.assertEqual(test_update.ddl(),
                     'ALTER TABLE {} DROP COLUMN value_2'.format(table_name))

  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.count')
  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_drop_column_error_on_primary_key(self, get_model, index_count):
    get_model.return_value = models.SmallTestModel
    index_count.return_value = 1

    test_update = update.DropColumn(models.SmallTestModel.table, 'key')
    with self.assertRaisesRegex(error.SpannerError, 'Column key is indexed'):
      test_update.validate()

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_create_table(self, get_model):
    get_model.return_value = None
    new_model = models.UnittestModel
    test_update = update.CreateTable(new_model)
    test_update.validate()

    test_model_ddl = ('CREATE TABLE table (int_ INT64 NOT NULL, int_2 INT64,'
                      ' float_ FLOAT64 NOT NULL, float_2 FLOAT64,'
                      ' string STRING(MAX) NOT NULL, string_2 STRING(MAX),'
                      ' timestamp TIMESTAMP NOT NULL, string_array'
                      ' ARRAY<STRING(MAX)>) PRIMARY KEY (int_, float_, string)')
    self.assertEqual(test_update.ddl(), test_model_ddl)

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_create_table_interleaved(self, get_model):
    get_model.return_value = None
    new_model = models.ChildTestModel
    test_update = update.CreateTable(new_model)
    test_update.validate()

    test_model_ddl = ('CREATE TABLE ChildTestModel ('
                      'key STRING(MAX) NOT NULL, '
                      'child_key STRING(MAX) NOT NULL) '
                      'PRIMARY KEY (key, child_key), '
                      'INTERLEAVE IN PARENT SmallTestModel ON DELETE CASCADE')
    self.assertEqual(test_update.ddl(), test_model_ddl)

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_create_table_error_on_existing_table(self, get_model):
    get_model.return_value = models.SmallTestModel
    new_model = models.SmallTestModel
    test_update = update.CreateTable(new_model)
    with self.assertRaisesRegex(error.SpannerError, 'already exists'):
      test_update.validate()

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.indexes')
  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.tables')
  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_drop_table(self, get_model, tables, indexes):
    table_name = models.RelationshipTestModel.table
    get_model.return_value = models.RelationshipTestModel
    tables.return_value = {}
    indexes.return_value = {}

    test_update = update.DropTable(table_name)
    test_update.validate()
    self.assertEqual(test_update.ddl(), 'DROP TABLE {}'.format(table_name))

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_add_index(self, get_model):
    table_name = models.SmallTestModel.table
    get_model.return_value = models.SmallTestModel

    test_update = update.CreateIndex(table_name, 'foo', ['value_1'])
    test_update.validate()
    self.assertEqual(test_update.ddl(),
                     'CREATE INDEX foo ON {} (value_1)'.format(table_name))


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

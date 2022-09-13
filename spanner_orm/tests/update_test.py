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

from absl.testing import parameterized

import spanner_orm
from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import update
from spanner_orm.testlib.spanner_emulator import testlib as spanner_emulator_testlib
from spanner_orm.tests import models


class UpdateTest(
    spanner_emulator_testlib.TestCase,
    parameterized.TestCase,
):

  def setUp(self):
    super().setUp()
    _, project_id = self.spanner_emulator_client.project_name.split('/')
    connection = spanner_orm.SpannerConnection(
        instance=self.spanner_emulator_instance.instance_id,
        database=self.spanner_emulator_database.database_id,
        project=project_id,
        credentials=self.spanner_emulator_client.credentials,
    )
    spanner_orm.from_connection(connection)
    spanner_orm.from_admin_connection(connection)

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
    new_model = models.UnittestModelWithoutSecondaryIndexes
    test_update = update.CreateTable(new_model)
    test_update.validate()

    test_model_ddl = ('CREATE TABLE table (int_ INT64 NOT NULL, int_2 INT64,'
                      ' float_ FLOAT64 NOT NULL, float_2 FLOAT64,'
                      ' string STRING(MAX) NOT NULL, string_2 STRING(MAX),'
                      ' string_3 STRING(20),'
                      ' bytes_ BYTES(MAX) NOT NULL, bytes_2 BYTES(MAX),'
                      ' bytes_3 BYTES(20),'
                      ' timestamp TIMESTAMP NOT NULL,'
                      ' string_array ARRAY<STRING(MAX)>,'
                      ' string_array_2 ARRAY<STRING(20)>)'
                      ' PRIMARY KEY (int_, float_, string, bytes_)')
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
  def test_create_table_foreign_key(self, get_model):
    self.maxDiff = 2000

    get_model.return_value = None
    new_model = models.ForeignKeyTestModel
    test_update = update.CreateTable(new_model)
    test_update.validate()

    test_model_ddl = (
        'CREATE TABLE ForeignKeyTestModel ('
        'referencing_key_1 STRING(MAX) NOT NULL, '
        'referencing_key_2 STRING(MAX) NOT NULL, '
        'referencing_key_3 INT64 NOT NULL, '
        'self_referencing_key STRING(MAX), '
        'CONSTRAINT foreign_key_1 FOREIGN KEY (referencing_key_1) '
        'REFERENCES SmallTestModel (key), '
        'CONSTRAINT foreign_key_2 '
        'FOREIGN KEY (referencing_key_2, referencing_key_3) '
        'REFERENCES table (string, int_), '
        'CONSTRAINT foreign_key_3 FOREIGN KEY (self_referencing_key) '
        'REFERENCES ForeignKeyTestModel (referencing_key_1)) '
        'PRIMARY KEY (referencing_key_1, referencing_key_2, referencing_key_3)')
    self.assertEqual(test_update.ddl(), test_model_ddl)

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_create_table_error_on_existing_table(self, get_model):
    get_model.return_value = models.SmallTestModel
    new_model = models.SmallTestModel
    test_update = update.CreateTable(new_model)
    with self.assertRaisesRegex(error.SpannerError, 'already exists'):
      test_update.validate()

  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_create_table_error_on_table_with_index(self, get_model):
    get_model.return_value = None
    new_model = models.IndexTestModel
    test_update = update.CreateTable(new_model)
    with self.assertRaisesRegex(
        error.SpannerError,
        'indexes cannot be created',
    ):
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

  @parameterized.named_parameters(
      (
          'basic',
          update.CreateIndex(
              table_name=models.SmallTestModel.table,
              index_name='foo',
              columns=['value_1'],
          ),
          f'CREATE INDEX foo ON {models.SmallTestModel.table} (value_1)',
      ),
      (
          'with_options',
          update.CreateIndex(
              table_name=models.SmallTestModel.table,
              index_name='foo',
              columns=['value_1'],
              null_filtered=True,
              unique=True,
          ),
          (f'CREATE UNIQUE NULL_FILTERED INDEX foo '
           f'ON {models.SmallTestModel.table} (value_1)'),
      ),
  )
  @mock.patch('spanner_orm.admin.metadata.SpannerMetadata.model')
  def test_add_index(self, test_update, expected_ddl, get_model):
    get_model.return_value = models.SmallTestModel

    test_update.validate()
    self.assertEqual(test_update.ddl(), expected_ddl)

  def test_execute_partitioned_dml(self):
    update.CreateTable(models.SmallTestModelWithoutSecondaryIndexes).execute()
    test_model = models.SmallTestModel(
        dict(
            key='some-key',
            value_1='foo',
            value_2='bar',
        ))
    test_model.save()
    update.ExecutePartitionedDml(
        "UPDATE SmallTestModel SET value_2 = value_1 WHERE value_2 = 'bar'",
    ).execute()
    test_model.reload()
    self.assertEqual(
        models.SmallTestModel(
            dict(
                key='some-key',
                value_1='foo',
                value_2='foo',
            )),
        test_model,
    )


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

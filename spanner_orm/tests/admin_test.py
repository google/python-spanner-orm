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

from spanner_orm.admin import column
from spanner_orm.admin import index
from spanner_orm.admin import index_column
from spanner_orm.admin import table
from spanner_orm.admin import metadata
from spanner_orm.tests import models


class AdminTest(unittest.TestCase):

  def make_test_tables(self, model, parent_table=None):
    tables = [{
        'table_catalog': '',
        'table_schema': '',
        'table_name': model.table,
        'parent_table_name': parent_table,
        'on_delete_action': None
    }]
    return [table.TableSchema(row) for row in tables]

  def make_test_columns(self, model):
    columns, iteration = [], 1
    for row in model.schema.values():
      columns.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': model.table,
          'column_name': row.name,
          'ordinal_position': iteration,
          'is_nullable': 'YES' if row.nullable() else 'NO',
          'spanner_type': row.field_type().ddl()
      })
      iteration += 1
    return [column.ColumnSchema(row) for row in columns]

  def make_test_index_columns(self, model):
    columns = []
    for row in model.primary_keys:
      columns.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': model.table,
          'index_name': 'PRIMARY_KEY',
          'column_name': row,
          'is_nullable': 'FALSE',
          'spanner_type': 'STRING'
      })
    return [index_column.IndexColumnSchema(row) for row in columns]

  def make_test_indexes(self, model):
    return [
        index.IndexSchema({
            'table_catalog': '',
            'table_schema': '',
            'table_name': model.table,
            'index_name': 'PRIMARY_KEY',
            'index_type': 'PRIMARY_KEY',
            'is_unique': True,
            'is_null_filtered': False,
            'index_state': 'READ_WRITE'
        })
    ]

  @mock.patch('spanner_orm.admin.index.IndexSchema.where')
  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.where')
  @mock.patch('spanner_orm.admin.column.ColumnSchema.where')
  @mock.patch('spanner_orm.admin.table.TableSchema.where')
  def test_metadata(self, tables, columns, index_columns, indexes):
    model = models.SmallTestModel
    tables.return_value = self.make_test_tables(model)
    columns.return_value = self.make_test_columns(model)
    index_columns.return_value = self.make_test_index_columns(model)
    indexes.return_value = self.make_test_indexes(model)

    meta = metadata.SpannerMetadata.models()[model.table]

    self.assertEqual(meta.table, model.table)
    self.assertEqual(meta.columns, model.columns)
    for row in model.columns:
      self.assertEqual(meta.schema[row].field_type(),
                       model.schema[row].field_type())
      self.assertEqual(meta.schema[row].nullable(),
                       model.schema[row].nullable())
    self.assertEqual(meta.primary_keys, model.primary_keys)

  @mock.patch('spanner_orm.admin.index.IndexSchema.where')
  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.where')
  @mock.patch('spanner_orm.admin.column.ColumnSchema.where')
  @mock.patch('spanner_orm.admin.table.TableSchema.where')
  def test_interleaved(self, tables, columns, index_columns, indexes):
    model = models.SmallTestModel
    parent_model = models.UnittestModel
    tables.return_value = (
        self.make_test_tables(model, parent_table=parent_model.table) +
        self.make_test_tables(parent_model))
    columns.return_value = (
        self.make_test_columns(model) + self.make_test_columns(parent_model))
    index_columns.return_value = (self.make_test_index_columns(model) + self.make_test_index_columns(parent_model))
    indexes.return_value = (self.make_test_indexes(model) + self.make_test_indexes(parent_model))

    meta = metadata.SpannerMetadata.models()['SmallTestModel']

    self.assertEqual(meta.table, model.table)
    self.assertEqual(meta.interleaved.table, parent_model.table)


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

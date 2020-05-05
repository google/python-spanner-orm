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

from spanner_orm import index
from spanner_orm.admin import column
from spanner_orm.admin import index as index_schema
from spanner_orm.admin import index_column
from spanner_orm.admin import metadata
from spanner_orm.admin import table
from spanner_orm.admin import update
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
    for row in model.fields.values():
      columns.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': model.table,
          'column_name': row.name,
          'ordinal_position': iteration,
          'is_nullable': 'YES' if row.nullable else 'NO',
          'spanner_type': row.field_type.ddl()
      })
      iteration += 1
    return [column.ColumnSchema(row) for row in columns]

  def make_test_index_columns(self, model, name=None, columns=None):
    name = name or index.Index.PRIMARY_INDEX
    columns = columns or model.primary_keys

    results, iteration = [], 1
    for row in columns:
      results.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': model.table,
          'index_name': name,
          'column_name': row,
          'ordinal_position': iteration,
          'column_ordering': 'ASC',
          'is_nullable': 'FALSE',
          'spanner_type': 'STRING'
      })
      iteration += 1
    return [index_column.IndexColumnSchema(row) for row in results]

  def make_test_index(self, model, name=None):
    if name is None:
      name = index.Index.PRIMARY_INDEX
      index_type = 'PRIMARY_KEY'
    else:
      index_type = 'INDEX'

    return [
        index_schema.IndexSchema({
            'table_catalog': '',
            'table_schema': '',
            'table_name': model.table,
            'index_name': name,
            'index_type': index_type,
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
    indexes.return_value = self.make_test_index(model)

    meta = metadata.SpannerMetadata.models()[model.table]

    self.assertEqual(meta.table, model.table)
    self.assertEqual(meta.columns, model.columns)
    for row in model.columns:
      self.assertEqual(meta.fields[row].field_type,
                       model.fields[row].field_type)
      self.assertEqual(meta.fields[row].nullable,
                       model.fields[row].nullable)
    self.assertEqual(meta.primary_keys, model.primary_keys)
    self.assertEqual(
        getattr(meta, index.Index.PRIMARY_INDEX).columns, model.primary_keys)

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
    index_columns.return_value = (
        self.make_test_index_columns(model) +
        self.make_test_index_columns(parent_model))
    indexes.return_value = (
        self.make_test_index(model) + self.make_test_index(parent_model))

    meta = metadata.SpannerMetadata.models()['SmallTestModel']

    self.assertEqual(meta.table, model.table)
    self.assertEqual(meta.interleaved.table, parent_model.table)

  @mock.patch('spanner_orm.admin.index.IndexSchema.where')
  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.where')
  @mock.patch('spanner_orm.admin.column.ColumnSchema.where')
  @mock.patch('spanner_orm.admin.table.TableSchema.where')
  def test_secondary_index(self, tables, columns, index_columns, indexes):
    model = models.SmallTestModel
    name = 'secondary_index'
    index_cols = ['value_1']
    tables.return_value = self.make_test_tables(model)
    columns.return_value = self.make_test_columns(model)
    index_columns.return_value = (
        self.make_test_index_columns(model) +
        self.make_test_index_columns(model, name=name, columns=index_cols))
    indexes.return_value = (
        self.make_test_index(model) + self.make_test_index(model, name=name))

    meta = metadata.SpannerMetadata.models()[model.table]
    self.assertEqual(meta.table, model.table)
    self.assertIn(name, meta.indexes)
    self.assertEqual(meta.indexes[name].columns, index_cols)
    self.assertEqual(getattr(meta, name).columns, index_cols)

  def test_model_creation_ddl(self):
      expected_ddl = [
          'CREATE TABLE IndexTestModel (key STRING(MAX) NOT NULL,'
          ' value STRING(MAX) NOT NULL) PRIMARY KEY (key)',
          'CREATE INDEX value ON IndexTestModel (value)'
      ]
      ddl = update.model_creation_ddl(models.IndexTestModel)
      self.assertEqual(ddl, expected_ddl)
      self.assertCountEqual(models.IndexTestModel.meta.indexes.keys(), ['PRIMARY_KEY', 'value_idx'])

  def test_model_creation_ddl2(self):
      expected_ddl = [
          'CREATE TABLE FieldCustomNameTestModel (key2 STRING(MAX) NOT NULL)'
          ' PRIMARY KEY (key2)'
      ]
      ddl = update.model_creation_ddl(models.FieldCustomNameTestModel)
      self.assertEqual(ddl, expected_ddl)
      self.assertCountEqual(models.FieldCustomNameTestModel.meta.indexes.keys(), ['PRIMARY_KEY'])


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

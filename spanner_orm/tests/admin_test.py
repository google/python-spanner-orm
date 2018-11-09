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
import logging
import unittest
from unittest import mock

from spanner_orm.admin import metadata
from spanner_orm.schemas import column
from spanner_orm.schemas import index
from spanner_orm.schemas import index_column
from spanner_orm.tests import models


class AdminTest(unittest.TestCase):

  def smalltestmodel_columns(self):
    columns, iteration = [], 1
    for row in models.SmallTestModel.meta.schema.values():
      columns.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': models.SmallTestModel.table(),
          'column_name': row.name,
          'ordinal_position': iteration,
          'is_nullable': 'YES' if row.nullable() else 'NO',
          'spanner_type': row.field_type().ddl()
      })
      iteration += 1
    return [column.ColumnSchema(row) for row in columns]

  def smalltestmodel_index_columns(self):
    columns = []
    for row in models.SmallTestModel.primary_index_keys():
      columns.append({
          'table_catalog': '',
          'table_schema': '',
          'table_name': models.SmallTestModel.table(),
          'index_name': 'PRIMARY_KEY',
          'column_name': row
      })
    return [index_column.IndexColumnSchema(row) for row in columns]

  def smalltestmodel_indexes(self):
    return [
        index.IndexSchema({
            'table_catalog': '',
            'table_schema': '',
            'table_name': models.SmallTestModel.table(),
            'index_name': 'PRIMARY_KEY',
            'index_type': 'PRIMARY_KEY',
            'is_unique': True,
            'index_state': 'READ_WRITE'
        })
    ]

  @mock.patch('spanner_orm.schemas.index.IndexSchema.where')
  @mock.patch('spanner_orm.schemas.index_column.IndexColumnSchema.where')
  @mock.patch('spanner_orm.schemas.column.ColumnSchema.where')
  def test_metadata(self, columns, index_columns, indexes):
    model = models.SmallTestModel
    columns.return_value = self.smalltestmodel_columns()
    index_columns.return_value = self.smalltestmodel_index_columns()
    indexes.return_value = self.smalltestmodel_indexes()

    meta = metadata.SpannerMetadata.models()['SmallTestModel']

    self.assertEqual(meta.table(), models.SmallTestModel.table())
    self.assertEqual(meta.schema().keys(), model.columns())
    for row in model.columns():
      self.assertEqual(meta.schema()[row].field_type(),
                       model.schema()[row].field_type())
      self.assertEqual(meta.schema()[row].nullable(),
                       model.schema()[row].nullable())
    self.assertEqual(meta.primary_index_keys(),
                     models.SmallTestModel.primary_index_keys())


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

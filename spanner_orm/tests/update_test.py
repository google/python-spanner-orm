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

from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import update
from spanner_orm.tests import models


class UpdateTest(unittest.TestCase):

  @mock.patch('spanner_orm.admin.update.SchemaUpdate._get_model')
  def test_column_update_add_column(self, get_model):
    get_model.return_value = models.SmallTestModel
    new_field = field.Field(field.String, nullable=True)
    test_update = update.ColumnUpdate('foo', 'bar', new_field)
    test_update.validate()
    self.assertEqual(test_update.ddl(),
                     'ALTER TABLE foo ADD COLUMN bar STRING(MAX)')

  @mock.patch('spanner_orm.admin.index_column.IndexColumnSchema.count')
  @mock.patch('spanner_orm.admin.update.SchemaUpdate._get_model')
  def test_column_update_error_on_primary_key(self, get_model, index_count):
    index_count.return_value = 1
    get_model.return_value = models.SmallTestModel
    test_update = update.ColumnUpdate(models.SmallTestModel.table, 'key', None)
    with self.assertRaisesRegex(AssertionError, 'indexed column'):
      test_update.validate()

  @mock.patch('spanner_orm.admin.update.SchemaUpdate._get_model')
  def test_create_table(self, get_model):
    get_model.return_value = None
    new_model = models.SmallTestModel
    test_update = update.CreateTableUpdate(new_model)
    self.assertEqual(test_update.ddl(), new_model.creation_ddl)

  @mock.patch('spanner_orm.admin.update.SchemaUpdate._get_model')
  def test_create_table_error_on_existing_table(self, get_model):
    get_model.return_value = models.SmallTestModel
    new_model = models.SmallTestModel
    test_update = update.CreateTableUpdate(new_model)
    with self.assertRaisesRegex(AssertionError, 'already exists'):
      test_update.validate()

if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

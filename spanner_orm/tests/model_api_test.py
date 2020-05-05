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
from spanner_orm.tests import models


class ModelApiTest(unittest.TestCase):

  @mock.patch('spanner_orm.table_apis.find')
  def test_find_calls_api(self, find):
    mock_transaction = mock.Mock()
    models.UnittestModel.find(mock_transaction, string='string', int_=1, float_=2.3)

    find.assert_called_once()
    (transaction, table, columns, keyset), _ = find.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.UnittestModel.table)
    self.assertEqual(columns, models.UnittestModel.columns)
    self.assertEqual(keyset.keys, [[1, 2.3, 'string']])

  @mock.patch('spanner_orm.table_apis.find')
  def test_find_result(self, find):
    mock_transaction = mock.Mock()

    find.return_value = [['key', 'value_1', None]]
    result = models.SmallTestModel.find(mock_transaction, key='key')
    if result:
      self.assertEqual(result.key, 'key')
      self.assertEqual(result.value_1, 'value_1')
      self.assertIsNone(result.value_2)
    else:
      self.fail('Failed to find result')

  @mock.patch('spanner_orm.table_apis.find')
  def test_find_multi_calls_api(self, find):
    mock_transaction = mock.Mock()
    models.UnittestModel.find_multi(mock_transaction, [{
        'string': 'string',
        'int_': 1,
        'float_': 2.3
    }])

    find.assert_called_once()
    (transaction, table, columns, keyset), _ = find.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.UnittestModel.table)
    self.assertEqual(columns, models.UnittestModel.columns)
    self.assertEqual(keyset.keys, [[1, 2.3, 'string']])

  @mock.patch('spanner_orm.table_apis.find')
  def test_find_multi_result(self, find):
    mock_transaction = mock.Mock()
    find.return_value = [['key', 'value_1', None]]
    results = models.SmallTestModel.find_multi(mock_transaction, [{
        'key': 'key'
    }])

    self.assertEqual(results[0].key, 'key')
    self.assertEqual(results[0].value_1, 'value_1')
    self.assertIsNone(results[0].value_2)

  @mock.patch('spanner_orm.table_apis.insert')
  def test_create_calls_api(self, insert):
    mock_transaction = mock.Mock()
    models.SmallTestModel.create(mock_transaction, key='key', value_1='value')

    insert.assert_called_once()
    (transaction, table, columns, values), _ = insert.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.SmallTestModel.table)
    self.assertEqual(list(columns), ['key', 'value_1'])
    self.assertEqual(list(values), [['key', 'value']])

  def test_create_error_on_invalid_keys(self):
    with self.assertRaises(error.SpannerError):
      models.SmallTestModel.create(key_2='key')

  def assert_api_called(self, mock_api, mock_transaction):
    mock_api.assert_called_once()
    (transaction, table, columns, values), _ = mock_api.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.SmallTestModel.table)
    self.assertEqual(list(columns), ['key', 'value_1', 'value_2'])
    self.assertEqual(list(values), [['key', 'value', None]])

  @mock.patch('spanner_orm.table_apis.insert')
  def test_save_batch_inserts(self, insert):
    mock_transaction = mock.Mock()
    values = {'key': 'key', 'value_1': 'value'}
    not_persisted = models.SmallTestModel(values)
    models.SmallTestModel.save_batch(mock_transaction, [not_persisted])
    self.assert_api_called(insert, mock_transaction)

  @mock.patch('spanner_orm.table_apis.update')
  def test_save_batch_updates(self, update):
    mock_transaction = mock.Mock()
    values = {'key': 'key', 'value_1': 'value'}
    persisted = models.SmallTestModel(values, persisted=True)
    models.SmallTestModel.save_batch(mock_transaction, [persisted])

    self.assert_api_called(update, mock_transaction)

  @mock.patch('spanner_orm.table_apis.upsert')
  def test_save_batch_force_write_upserts(self, upsert):
    mock_transaction = mock.Mock()
    values = {'key': 'key', 'value_1': 'value'}
    not_persisted = models.SmallTestModel(values)
    models.SmallTestModel.save_batch(
        mock_transaction, [not_persisted], force_write=True)
    self.assert_api_called(upsert, mock_transaction)

  @mock.patch('spanner_orm.table_apis.delete')
  def test_delete_batch_deletes(self, delete):
    mock_transaction = mock.Mock()
    values = {'key': 'key', 'value_1': 'value'}
    model = models.SmallTestModel(values)
    models.SmallTestModel.delete_batch(mock_transaction, [model])

    delete.assert_called_once()
    (transaction, table, keyset), _ = delete.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.SmallTestModel.table)
    self.assertEqual(keyset.keys, [[model.key]])


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

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
import datetime
import logging
from typing import List
import unittest
from unittest import mock

from absl.testing import parameterized
from spanner_orm import error
from spanner_orm import field
from spanner_orm.tests import models


class ModelTest(parameterized.TestCase):

  @mock.patch('spanner_orm.table_apis.find')
  def test_find_calls_api(self, find):
    mock_transaction = mock.Mock()
    models.UnittestModel.find(
        mock_transaction, string='string', int_=1, float_=2.3)

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

  def test_set_attr(self):
    test_model = models.SmallTestModel({'key': 'key', 'value_1': 'value'})
    test_model.value_1 = 'value_1'
    test_model.value_2 = 'value_2'
    self.assertEqual(test_model.values, {
        'key': 'key',
        'value_1': 'value_1',
        'value_2': 'value_2',
    })

  def test_set_error_on_primary_key(self):
    test_model = models.SmallTestModel({'key': 'key', 'value_1': 'value'})
    with self.assertRaises(AttributeError):
      test_model.key = 'error'

  @parameterized.parameters(('int_2', 'foo'), ('float_2', 'bar'),
                            ('string_2', 5), ('string_array', 'foo'),
                            ('timestamp', 5))
  def test_set_error_on_invalid_type(self, attribute, value):
    string_array = ['foo', 'bar']
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    test_model = models.UnittestModel({
        'int_': 0,
        'float_': 0,
        'string': '',
        'string_array': string_array,
        'timestamp': timestamp
    })
    with self.assertRaises(AttributeError):
      setattr(test_model, attribute, value)

  def test_get_attr(self):
    test_model = models.SmallTestModel({'key': 'key', 'value_1': 'value'})
    self.assertEqual(test_model.key, 'key')
    self.assertEqual(test_model.value_1, 'value')
    self.assertEqual(test_model.value_2, None)

  @parameterized.parameters(
      (True, True),
      (True, False),
      (False, True),
  )
  def test_skip_validation(self, persisted, skip_validation):
    models.SmallTestModel(
        {'value_1': 'value'},
        persisted=persisted,
        skip_validation=skip_validation,
    )

  def test_validation(self):
    with self.assertRaises(error.SpannerError):
      models.SmallTestModel(
          {'value_1': 'value'},
          persisted=False,
          skip_validation=False,
      )

  def test_model_equates(self):
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    test_model1 = models.UnittestModel({
        'int_': 0,
        'float_': 0,
        'string': '',
        'string_array': ['foo', 'bar'],
        'timestamp': timestamp,
    })
    test_model2 = models.UnittestModel({
        'int_': 0,
        'float_': 0.0,
        'string': '',
        'string_array': ['foo', 'bar'],
        'timestamp': timestamp,
    })
    self.assertEqual(test_model1, test_model2)

  @parameterized.parameters(
      (models.UnittestModel({
          'int_': 0,
          'float_': 0,
          'string': '1',
          'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
      }),
       models.UnittestModel({
           'int_': 0,
           'float_': 0,
           'string': 'a',
           'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
       })),
      (models.UnittestModel({
          'int_': 0,
          'float_': 0,
          'string': '',
          'string_array': ['foo', 'bar'],
          'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
      }),
       models.UnittestModel({
           'int_': 0,
           'float_': 0,
           'string': '',
           'string_array': ['bar', 'foo'],
           'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
       })),
  )
  def test_model_are_different(self, test_model1, test_model2):
    self.assertNotEqual(test_model1, test_model2)

  def test_id(self):
    primary_key = {'string': 'foo', 'int_': 5, 'float_': 2.3}
    all_data = primary_key.copy()
    all_data.update({
        'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
        'string_array': ['foo', 'bar']
    })
    test_model = models.UnittestModel(all_data)
    self.assertEqual(test_model.id(), primary_key)

  def test_changes(self):
    test_model = models.SmallTestModel({'key': 'key', 'value_1': 'value'})
    test_model.value_1 = 'change'
    self.assertEqual(test_model.changes(), {'value_1': 'change'})

    test_model.value_1 = 'value'
    self.assertEqual(test_model.changes(), {})

  def test_object_changes(self):
    array = ['foo', 'bar']
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    test_model = models.UnittestModel({
        'int_': 0,
        'float_': 0,
        'string': '',
        'string_array': array,
        'timestamp': timestamp
    })

    # Make sure that changing an object on the model shows up in changes()
    string_array = test_model.string_array  # type: List
    string_array.append('bat')
    self.assertIn('string_array', test_model.changes())

  def test_field_exists_on_model_class(self):
    self.assertIsInstance(models.SmallTestModel.key, field.Field)
    self.assertEqual(models.SmallTestModel.key.field_type(), field.String)
    self.assertFalse(models.SmallTestModel.key.nullable())
    self.assertEqual(models.SmallTestModel.key.name, 'key')

  def test_field_inheritance(self):
    self.assertEqual(models.InheritanceTestModel.key, models.SmallTestModel.key)

    values = {'key': 'key', 'value_1': 'value_1', 'value_3': 'value_3'}
    test_model = models.InheritanceTestModel(values)
    for name, value in values.items():
      self.assertEqual(getattr(test_model, name), value)

  def test_relation_get(self):
    test_model = models.RelationshipTestModel({
        'parent_key': 'parent',
        'child_key': 'child',
        'parent': []
    })
    self.assertEqual(test_model.parent, [])

  def test_relation_get_error_on_unretrieved(self):
    test_model = models.RelationshipTestModel({
        'parent_key': 'parent',
        'child_key': 'child'
    })
    with self.assertRaises(AttributeError):
      _ = test_model.parent

  def test_interleaved(self):
    self.assertEqual(models.ChildTestModel.interleaved, models.SmallTestModel)

  @mock.patch('spanner_orm.model.Model.find')
  def test_reload(self, find):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)

    find.return_value = None
    self.assertIsNone(model.reload())
    find.assert_called_once()
    (transaction,), kwargs = find.call_args
    self.assertIsNone(transaction)
    self.assertEqual(kwargs, model.id())

  @mock.patch('spanner_orm.model.Model.find')
  def test_reload_reloads(self, find):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)

    updated_values = {'key': 'key', 'value_1': 'value_2'}
    find.return_value = models.SmallTestModel(updated_values)
    model.reload()
    self.assertEqual(model.value_1, updated_values['value_1'])
    self.assertEqual(model.changes(), {})

  @mock.patch('spanner_orm.model.Model.create')
  def test_save_creates(self, create):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)
    model.save()

    create.assert_called_once()
    (transaction,), kwargs = create.call_args
    self.assertIsNone(transaction)
    self.assertEqual(kwargs, {**values, 'value_2': None})

  @mock.patch('spanner_orm.model.Model.update')
  def test_save_updates(self, update):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=True)

    values['value_1'] = 'new_value'
    model.value_1 = values['value_1']
    model.save()

    update.assert_called_once()
    (transaction,), kwargs = update.call_args
    self.assertIsNone(transaction)
    self.assertEqual(kwargs, values)

  @mock.patch('spanner_orm.model.Model.update')
  def test_save_no_changes(self, update):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=True)
    model.save()
    update.assert_not_called()

  @mock.patch('spanner_orm.table_apis.delete')
  def test_delete_deletes(self, delete):
    mock_transaction = mock.Mock()
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values)
    model.delete(mock_transaction)

    delete.assert_called_once()
    (transaction, table, keyset), _ = delete.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.SmallTestModel.table)
    self.assertEqual(keyset.keys, [[model.key]])


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

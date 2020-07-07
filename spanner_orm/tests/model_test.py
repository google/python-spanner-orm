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
from spanner_orm import field
from spanner_orm.tests import models


class ModelTest(parameterized.TestCase):

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

  @parameterized.parameters(('int_2', 'foo'), ('float_2', 'bar'), ('string_2', 5),
                            ('string_array', 'foo'), ('timestamp', 5))
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

  @mock.patch('spanner_orm.model.ModelApi.find')
  def test_reload(self, find):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)

    find.return_value = None
    self.assertIsNone(model.reload())
    find.assert_called_once()
    (transaction,), kwargs = find.call_args
    self.assertIsNone(transaction)
    self.assertEqual(kwargs, model.id())

  @mock.patch('spanner_orm.model.ModelApi.find')
  def test_reload_reloads(self, find):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)

    updated_values = {'key': 'key', 'value_1': 'value_2'}
    find.return_value = models.SmallTestModel(updated_values)
    model.reload()
    self.assertEqual(model.value_1, updated_values['value_1'])
    self.assertEqual(model.changes(), {})

  @mock.patch('spanner_orm.model.ModelApi.create')
  def test_save_creates(self, create):
    values = {'key': 'key', 'value_1': 'value_1'}
    model = models.SmallTestModel(values, persisted=False)
    model.save()

    create.assert_called_once()
    (transaction,), kwargs = create.call_args
    self.assertIsNone(transaction)
    self.assertEqual(kwargs, {**values, 'value_2': None})

  @mock.patch('spanner_orm.model.ModelApi.update')
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

  @mock.patch('spanner_orm.model.ModelApi.update')
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

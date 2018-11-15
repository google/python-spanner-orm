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

import datetime
import logging
import unittest

from spanner_orm import error
from spanner_orm import field
from spanner_orm.tests import models


class ModelTest(unittest.TestCase):

  def test_set_attr_item(self):
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    string_array = ['foo', 'bar']

    test_model = models.UnittestModel({'int_': 0, 'string': ''})
    test_model.timestamp = timestamp
    test_model.string_array = string_array
    self.assertEqual(
        test_model.values, {
            'int_': 0,
            'string': '',
            'string_array': string_array,
            'timestamp': timestamp
        })

  def test_cannot_set_primary_keys(self):
    test_model = models.UnittestModel({'int_': 0, 'string': ''})
    with self.assertRaises(AttributeError):
      test_model.int_ = 2

  def test_get_attr(self):
    test_model = models.UnittestModel({'int_': 5, 'string': 'foo'})
    self.assertEqual(test_model.int_, 5)
    self.assertEqual(test_model.string, 'foo')
    self.assertEqual(test_model.timestamp, None)

  def test_cannot_set_invalid_type(self):
    test_model = models.UnittestModel({'int_': 0, 'string': ''})
    with self.assertRaises(AttributeError):
      test_model.int_2 = 'foo'

    with self.assertRaises(AttributeError):
      test_model.string_2 = 5

    with self.assertRaises(AttributeError):
      test_model.string_array = 'foo'

    with self.assertRaises(AttributeError):
      test_model.timestamp = 5

  def test_id(self):
    primary_key = {'string': 'foo', 'int_': 5}
    all_data = primary_key.copy()
    all_data.update({
        'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
        'string_array': ['foo', 'bar']
    })
    test_model = models.UnittestModel(all_data)
    self.assertEqual(test_model.id(), primary_key)

    with self.assertRaises(error.SpannerError):
      models.UnittestModel({})

  def test_changes(self):
    test_model = models.UnittestModel({'int_': 0, 'string': '', 'int_2': 0})
    test_model.int_2 = 5
    self.assertEqual(test_model.changes(), {'int_2': 5})

    test_model.int_2 = 0
    self.assertEqual(test_model.changes(), {})

  def test_object_changes(self):
    array = ['foo', 'bar']
    test_model = models.UnittestModel({
        'int_': 0,
        'string': '',
        'string_array': array
    })

    # Make sure that changes to object off of model don't change the model
    array.append('bat')
    self.assertNotEqual(test_model.string_array, array)

    # Make sure that changing an object on the model shows up in changes()
    test_model.string_array.append('bat')
    self.assertIn('string_array', test_model.changes())

  def test_create_statement(self):
    test_model_ddl = ('CREATE TABLE table (int_ INT64 NOT NULL, int_2 INT64,'
                      ' string STRING(MAX) NOT NULL, string_2 STRING(MAX),'
                      ' timestamp TIMESTAMP NOT NULL, string_array'
                      ' ARRAY<STRING(MAX)>) PRIMARY KEY (int_, string)')
    self.assertEqual(models.UnittestModel.creation_ddl, test_model_ddl)

  def test_model_class_attribute(self):
    self.assertIsInstance(models.SmallTestModel.key, field.Field)
    self.assertEqual(models.SmallTestModel.key.field_type(), field.String)
    self.assertFalse(models.SmallTestModel.key.nullable())
    self.assertEqual(models.SmallTestModel.key.name, 'key')

  def test_relation_get(self):
    test_model = models.ChildTestModel({
        'parent_key': 'parent',
        'child_key': 'child',
        'parent': []
    })
    self.assertEqual(test_model.parent, [])

  def test_error_on_unretrieved_relation_get(self):
    test_model = models.ChildTestModel({
        'parent_key': 'parent',
        'child_key': 'child'
    })
    with self.assertRaises(AttributeError):
      _ = test_model.parent

  def test_inherited_fields(self):
    self.assertEqual(models.InheritanceTestModel.key, models.SmallTestModel.key)

    values = {'key': 'key', 'value_3': 'value_3'}
    test_model = models.InheritanceTestModel(values)
    for name, value in values.items():
      self.assertEqual(getattr(test_model, name), value)

if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

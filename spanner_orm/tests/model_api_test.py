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

import unittest
from unittest import mock

from spanner_orm import api
from spanner_orm import error
from spanner_orm.tests import models

from google.cloud import spanner

class ModelTest(unittest.TestCase):

  def test_find_error_on_invalid_keys(self):
    with self.assertRaises(error.SpannerError):
      models.UnittestModel.find(int_=1)

  @mock.patch('spanner_orm.api.SpannerApi.find')
  def test_find_calls_api(self, find):
    mock_transaction = mock.Mock()
    models.UnittestModel.find(mock_transaction, string='string', int_=1)

    find.assert_called_once()
    (transaction, table, columns, keyset), _ = find.call_args
    self.assertEqual(transaction, mock_transaction)
    self.assertEqual(table, models.UnittestModel.table)
    self.assertEqual(columns, models.UnittestModel.columns)
    self.assertEqual(keyset.keys, [[1, 'string']])

  @mock.patch('spanner_orm.api.SpannerApi.find')
  def test_find_result(self, find):
    mock_transaction = mock.Mock()
    find.return_value = [['key', 'value_1', None]]
    result = models.SmallTestModel.find(mock_transaction, key='key')

    self.assertEqual(result.key, 'key')
    self.assertEqual(result.value_1, 'value_1')
    self.assertIsNone(result.value_2)

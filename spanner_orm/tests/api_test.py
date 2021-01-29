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

from absl.testing import parameterized
from google.api_core import exceptions
from spanner_orm import api
from spanner_orm import error
from spanner_orm.admin import api as admin_api


def _mock_run_in_transaction(method, *args, **kwargs):
  return method(*args, **kwargs)


class MockSpannerApi(api.SpannerReadApi, api.SpannerWriteApi):

  def __init__(self):
    self.connection_mock = mock.MagicMock()
    self.connection_mock.run_in_transaction.side_effect = _mock_run_in_transaction

  @property
  def _connection(self):
    return self.connection_mock


class ApiTest(parameterized.TestCase):

  @mock.patch('google.cloud.spanner.Client')
  def test_api_connection(self, client):
    connection = self.mock_connection(client)
    api.connect('', '', '')
    self.assertEqual(api.spanner_api()._connection, connection)

    api.hangup()
    with self.assertRaises(error.SpannerError):
      api.spanner_api()

  def test_api_error_when_not_connected(self):
    with self.assertRaises(error.SpannerError):
      api.spanner_api()

  @mock.patch('google.cloud.spanner.Client')
  def test_admin_api_connection(self, client):
    connection = self.mock_connection(client)
    admin_api.connect('', '', '')
    self.assertEqual(admin_api.spanner_admin_api()._connection, connection)

    admin_api.hangup()
    with self.assertRaises(error.SpannerError):
      admin_api.spanner_admin_api()

  @mock.patch('google.cloud.spanner.Client')
  def test_admin_api_create_ddl_connection(self, client):
    connection = self.mock_connection(client)
    admin_api.connect('', '', '', create_ddl=['create ddl'])
    self.assertEqual(admin_api.spanner_admin_api()._connection, connection)

  @parameterized.parameters('run_read_only', 'run_write')
  @mock.patch('spanner_orm.api.spanner_api')
  def test_reconnect_on_expected_error(self, api_method, mock_spanner_api):
    mock_api = MockSpannerApi()

    mock_method = mock.Mock()
    mock_method.side_effect = [
        exceptions.NotFound('Session not found'),
        'Anything other than an exception'
    ]
    mock_connect = mock_spanner_api.return_value.connect

    getattr(mock_api, api_method)(mock_method)

    mock_connect.assert_called_once()
    mock_method.assert_called()

  @parameterized.parameters('run_read_only', 'run_write')
  @mock.patch('spanner_orm.api.spanner_api')
  def test_raise_on_expected_error(self, api_method, mock_spanner_api):
    mock_api = MockSpannerApi()

    mock_method = mock.Mock()
    mock_method.side_effect = exceptions.NotFound('Database not found')

    with self.assertRaises(exceptions.NotFound):
      getattr(mock_api, api_method)(mock_method)

    mock_method.assert_called()

  def mock_connection(self, client):
    connection = mock.Mock()
    client().instance().database.return_value = connection
    return connection


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

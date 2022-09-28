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
import warnings

from absl.testing import parameterized
from google.api_core import exceptions
from google.cloud import spanner

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

  @mock.patch.object(spanner, 'Client', autospec=True, spec_set=True)
  def test_connection_args(self, client):
    client.return_value.instance.return_value.database.return_value = (
        'fake-database')
    connection = api.SpannerConnection(
        instance='some-instance',
        database='some-database',
        project='some-project',
        credentials='fake-credentials',
        pool='fake-pool',
        create_ddl=('fake-ddl',),
        client_options=dict(fake='options'),
    )
    self.assertEqual('fake-database', connection.database)
    self.assertSequenceEqual(
        (
            mock.call(
                project='some-project',
                credentials='fake-credentials',
                client_options=dict(fake='options'),
            ),
            mock.call().instance('some-instance'),
            mock.call().instance().database(
                'some-database',
                pool='fake-pool',
                ddl_statements=('fake-ddl',),
            ),
        ),
        client.mock_calls,
    )

  @mock.patch('google.cloud.spanner.Client')
  def test_api_connection(self, client):
    connection = self.mock_connection(client)
    with warnings.catch_warnings(record=True) as connect_warnings:
      api.connect('', '', '')
    self.assertEqual(api.spanner_api()._connection, connection)
    self.assertLen(connect_warnings, 1)
    connect_warning, = connect_warnings
    self.assertIn('spanner_orm.from_connection', str(connect_warning.message))
    self.assertIs(DeprecationWarning, connect_warning.category)
    self.assertEqual(api.__file__, connect_warning.filename)

    api.hangup()
    with self.assertRaises(error.SpannerError):
      api.spanner_api()

  def test_api_error_when_not_connected(self):
    with self.assertRaises(error.SpannerError):
      api.spanner_api()

  @mock.patch('google.cloud.spanner.Client')
  def test_admin_api_connection(self, client):
    connection = self.mock_connection(client)
    with warnings.catch_warnings(record=True) as connect_warnings:
      admin_api.connect('', '', '')
    self.assertEqual(admin_api.spanner_admin_api()._connection, connection)
    self.assertLen(connect_warnings, 1)
    connect_warning, = connect_warnings
    self.assertIn('spanner_orm.from_admin_connection',
                  str(connect_warning.message))
    self.assertIs(DeprecationWarning, connect_warning.category)
    self.assertEqual(admin_api.__file__, connect_warning.filename)

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
    mock_connect = mock_spanner_api.return_value.spanner_connection.connect

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

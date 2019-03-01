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

from spanner_orm import api
from spanner_orm import error
from spanner_orm.admin import api as admin_api


class ApiTest(unittest.TestCase):

  @mock.patch('google.cloud.spanner.Client')
  def test_api_connection(self, client):
    connection = self.mock_connection(client)
    api.SpannerApi.connect('', '', '')
    self.assertEqual(api.SpannerApi._connection(), connection)

    api.SpannerApi.hangup()
    with self.assertRaises(error.SpannerError):
      api.SpannerApi._connection()

  def test_api_error_when_not_connected(self):
    with self.assertRaises(error.SpannerError):
      api.SpannerApi.run_read_only(None)

    with self.assertRaises(error.SpannerError):
      api.SpannerApi.run_write(None)

  @mock.patch('google.cloud.spanner.Client')
  def test_admin_api_connection(self, client):
    connection = self.mock_connection(client)
    admin_api.SpannerAdminApi.connect('', '', '')
    self.assertEqual(admin_api.SpannerAdminApi._connection(), connection)

    admin_api.SpannerAdminApi.hangup()
    with self.assertRaises(error.SpannerError):
      api.SpannerApi._connection()

  @mock.patch('google.cloud.spanner.Client')
  def test_admin_api_create_ddl_connection(self, client):
    connection = self.mock_connection(client)
    admin_api.SpannerAdminApi.connect('', '', '', create_ddl=['create ddl'])
    self.assertEqual(admin_api.SpannerAdminApi._connection(), connection)

  def mock_connection(self, client):
    connection = mock.Mock()
    client().instance().database.return_value = connection
    return connection


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

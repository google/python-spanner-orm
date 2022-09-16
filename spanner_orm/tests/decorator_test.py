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
from spanner_orm import decorator


class DecoratorTest(parameterized.TestCase):

  @parameterized.parameters(
      (decorator.transactional_read, 'run_read_only'),
      (decorator.transactional_write, 'run_write'),
  )
  @mock.patch('spanner_orm.api.spanner_api')
  def test_transactional_injects_new_transaction(self, decorator_in_test,
                                                 method_name_to_mock,
                                                 mock_spanner_api):
    mock_tx = mock.Mock()
    mock_api_method = getattr(mock_spanner_api.return_value,
                              method_name_to_mock)
    mock_api_method.side_effect = mock_spanner_method(mock_tx)

    @decorator_in_test
    def get_book(book_id, method=None, transaction=None):
      self.assertEqual(mock_tx, transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('library', method)

      return 200

    result = get_book(123, method='library')
    self.assertEqual(200, result)
    mock_api_method.assert_called_once()

  @parameterized.parameters(decorator.transactional_read,
                            decorator.transactional_write)
  def test_transactional_uses_given_transaction(self, decorator_in_test):
    mock_tx = mock.Mock()

    @decorator_in_test
    def get_book(book_id, method=None, transaction=None):
      self.assertEqual(mock_tx, transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('library', method)

      return 200

    result = get_book(123, method='library', transaction=mock_tx)

    self.assertEqual(200, result)


def mock_spanner_method(mock_transaction):

  def _mock_spanner_method(method, *args, **kwargs):
    return method(mock_transaction, *args, **kwargs)

  return _mock_spanner_method


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

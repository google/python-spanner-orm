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
from unittest import mock

from absl.testing import parameterized
from spanner_orm import decorator


class DecoratorTest(parameterized.TestCase):

  @parameterized.parameters(
      (decorator.transactional_read, 'run_read_only'),
      (decorator.transactional_write, 'run_write'),
  )
  @mock.patch('spanner_orm.api.SpannerApi', autospec=True)
  def test_transactional_injects_new_transaction(
      self, decorator_in_test, method_name_to_mock, mock_spanner_api):
    mock_tx = mock.Mock()
    mock_api_method = getattr(mock_spanner_api, method_name_to_mock)
    mock_api_method.side_effect = mock_spanner_method(mock_tx)

    def get_book(transaction, book_id, genre=None):
      self.assertEqual(mock_tx, transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('horror', genre)

      return 200

    # decorate it
    decorated_get_book = decorator_in_test(get_book)

    result = decorated_get_book(123, genre='horror')
    self.assertEqual(200, result)
    mock_api_method.assert_called_once_with(
        get_book, 123, genre='horror')

  @parameterized.parameters(decorator.transactional_read,
                            decorator.transactional_write)
  def test_transactional_uses_given_transaction(self, decorator_in_test):
    mock_tx = mock.Mock()

    @decorator_in_test
    def get_book(transaction, book_id, genre=None):
      self.assertEqual(mock_tx, transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('horror', genre)

      return 200

    result = get_book(123, genre='horror', transaction=mock_tx)  # pylint: disable=redundant-keyword-arg, no-value-for-parameter

    self.assertEqual(200, result)

  @parameterized.parameters(decorator.transactional_read,
                            decorator.transactional_write)
  def test_transactional_overwrites_user_defined_transaction_kwarg(
      self, decorator_in_test):
    mock_my_own_tx = mock.Mock()

    @decorator_in_test
    def get_book(tx, book_id, genre=None, transaction=None):
      self.assertEqual(tx, mock_my_own_tx)

      # my own transaction kwarg is overwritten by decorator
      self.assertIsNone(transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('horror', genre)

      return 200

    result = get_book(123, genre='horror', transaction=mock_my_own_tx)  # pylint: disable=no-value-for-parameter

    self.assertEqual(200, result)


def mock_spanner_method(mock_transaction):

  def _mock_spanner_method(method, *args, **kwargs):
    return method(mock_transaction, *args, **kwargs)

  return _mock_spanner_method

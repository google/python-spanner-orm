from unittest import mock

from absl.testing import parameterized
from spanner_orm import decorator


# pylint: disable=redundant-keyword-arg
# pylint: disable=no-value-for-parameter
class DecoratorTest(parameterized.TestCase):

  @parameterized.parameters(
      (decorator.transactional_read, 'run_read_only'),
      (decorator.transactional_write, 'run_write'),
  )
  @mock.patch('spanner_orm.api.SpannerApi', autospec=True)
  def test_transactional_injects_new_transaction(self, decorator_in_test,
                                                 api_to_mock, mock_api):
    mock_tx = mock.Mock()
    mock_api_method = getattr(mock_api, api_to_mock)
    mock_api_method.side_effect = mock_spanner_method(mock_tx)

    def get_book(transaction, book_id, genre=None):
      self.assertEqual(mock_tx, transaction)
      self.assertEqual(123, book_id)
      self.assertEqual('horror', genre)

      return 200

    # save pre decorated function to be able to assert it later
    get_book_pre_decorator = get_book

    # decorate it
    get_book = decorator_in_test(get_book)

    result = get_book(123, genre='horror')
    self.assertEqual(200, result)
    mock_api_method.assert_called_once_with(
        get_book_pre_decorator, 123, genre='horror')

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

    result = get_book(123, genre='horror', transaction=mock_tx)
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

    result = get_book(123, genre='horror', transaction=mock_my_own_tx)
    self.assertEqual(200, result)


def mock_spanner_method(mock_transaction):

  def _mock_spanner_method(method, *args, **kwargs):
    return method(mock_transaction, *args, **kwargs)

  return _mock_spanner_method

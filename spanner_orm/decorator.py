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
"""Transaction decorators."""

from typing import Callable, TypeVar

from spanner_orm import api

T = TypeVar('T')


def transactional_read(func: Callable[..., T]) -> Callable[..., T]:
  """Injects a read-only transaction as keyword argument to given function.

    For example:

    @transactional_read
    def get_book(book_id, transaction=None):
      return Book(book_id)

    Client would then call this by skipping the 'transaction' argument:
    book_reader.get_book(123)

    To call a decorated function if you already have a transaction:

    @transactional_read
    def list_books(book_ids, transaction=None):
      for book_id in book_ids:
        get_book(book_id, transaction=transaction)

    list_books method would call get_book method using same transaction

  Args:
    func: Callable which will be called with read-only transaction and original
      arguments. Decorated `func` can also be passed an optional 'transaction'
      kwarg to use a given transaction, instead of creating a new one.

  Returns:
    decorated function
  """
  api_method_lambda = lambda: api.spanner_api().run_read_only
  return _transactional(api_method_lambda, func)


def transactional_write(func: Callable[..., T]) -> Callable[..., T]:
  """Injects a write transaction object as first argument to given function.

    For example:

    @transactional_write
    def save_book(book_id, transaction=None):
      ...

    Client would then call this by skipping the 'transaction' argument:
    book_reader.save_book(123)

    To call a decorated function if you already have a transaction:

    @transactional_write
    def save_books(book_ids, transaction=None):
      for book_id in book_ids:
        save_book(book_id, transaction=transaction)

    save_books method would call save_book method using same transaction

  Args:
    func: Callable which will be called with write transaction and original
      arguments. Decorated `func` can also be passed an optional 'transaction'
      kwarg to use a given transaction, instead of creating a new one.

  Returns:
    decorated function
  """
  api_method_lambda = lambda: api.spanner_api().run_write
  return _transactional(api_method_lambda, func)


def _transactional(spanner_api_method_lambda: Callable[[], Callable[..., T]],
                   func: Callable[..., T]) -> Callable[..., T]:
  """Returns decorated function."""

  # Spanner library calls given function with transaction as first argument.
  # It will call 'spanner_wrapper', and we will move transaction from first
  # argument to 'transaction' kwarg and call actual 'func'

  def spanner_wrapper(transaction, *args, **kwargs) -> T:
    return func(*args, transaction=transaction, **kwargs)

  def wrapper(*args, **kwargs) -> T:
    if 'transaction' in kwargs:
      return func(*args, **kwargs)

    spanner_api_method = spanner_api_method_lambda()
    return spanner_api_method(spanner_wrapper, *args, **kwargs)

  return wrapper

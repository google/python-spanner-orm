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
"""Class that handles API calls to Spanner."""

from __future__ import annotations

import abc
import logging
from typing import Any, Callable, Iterable, List, Optional, TypeVar

from spanner_orm import error

from google.auth import credentials as auth_credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database as spanner_database
from google.cloud.spanner_v1 import transaction as spanner_transaction

_logger = logging.getLogger(__name__)

CallableReturn = TypeVar('CallableReturn')


class SpannerReadApi(abc.ABC):
  """Handles sending read requests to Spanner."""

  @classmethod
  @abc.abstractmethod
  def _connection(cls) -> spanner_database.Database:
    raise NotImplementedError

  @classmethod
  def run_read_only(cls, method: Callable[..., CallableReturn], *args: Any,
                    **kwargs: Any) -> CallableReturn:
    """Wraps read-only queries in a read transaction."""
    with cls._connection().snapshot(multi_use=True) as snapshot:
      return method(snapshot, *args, **kwargs)

  # Read methods
  @staticmethod
  def find(transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str],
           keyset: spanner.KeySet) -> List[Iterable[Any]]:
    """Obtains rows with primary_keys from the given table."""
    _logger.debug('Find table=%s columns=%s keys=%s', table_name, columns,
                  keyset.keys)
    stream_results = transaction.read(
        table=table_name, columns=columns, keyset=keyset)
    return list(stream_results)

  @staticmethod
  def sql_query(transaction: spanner_transaction.Transaction, query: str,
                parameters: Iterable[str],
                parameter_types: Iterable[Any]) -> List[Iterable[Any]]:
    """Runs a read only SQL query."""
    _logger.debug('Executing SQL:\n%s\n%s\n%s', query, parameters,
                  parameter_types)
    stream_results = transaction.execute_sql(
        query, params=parameters, param_types=parameter_types)
    return list(stream_results)


class SpannerWriteApi(abc.ABC):
  """Handles sending write requests to Spanner."""

  @classmethod
  @abc.abstractmethod
  def _connection(cls) -> spanner_database.SpannerDatabase:
    raise NotImplementedError

  @classmethod
  def run_write(cls, method: Callable[..., CallableReturn], *args: Any,
                **kwargs: Any) -> CallableReturn:
    """Wraps write and read-write queries in a transaction."""
    return cls._connection().run_in_transaction(method, *args, **kwargs)

  @staticmethod
  def delete(transaction: spanner_transaction.Transaction, table_name: str,
             keyset: spanner.KeySet) -> None:
    _logger.debug('Delete table=%s keys=%s', table_name, keyset.keys)
    transaction.delete(table=table_name, keyset=keyset)

  # Write methods
  @staticmethod
  def insert(transaction: spanner_transaction.Transaction, table_name: str,
             columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
    """Add rows to a table."""
    _logger.debug('Insert table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.insert(table=table_name, columns=columns, values=values)

  @staticmethod
  def update(transaction: spanner_transaction.Transaction, table_name: str,
             columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
    """Updates rows of a table."""
    _logger.debug('Update table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.update(table=table_name, columns=columns, values=values)

  @staticmethod
  def upsert(transaction: spanner_transaction.Transaction, table_name: str,
             columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
    """Updates existing rows of a table or adds rows if they don't exist."""
    _logger.debug('Upsert table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.insert_or_update(
        table=table_name, columns=columns, values=values)


class SpannerApi(SpannerReadApi, SpannerWriteApi):
  """Class that handles reading from and writing to Spanner tables."""

  _connection_info = None
  _spanner_connection = None

  @classmethod
  def _connection(cls) -> spanner_database.SpannerDatabase:
    if not cls._spanner_connection:
      raise error.SpannerError('Not connected to spanner')
    return cls._spanner_connection

  # Spanner connection methods
  @classmethod
  def connect(cls,
              instance: str,
              database: str,
              project: Optional[str] = None,
              credentials: Optional[auth_credentials.Credentials] = None,
              pool: Optional[spanner.Pool] = None) -> None:
    """Connects to the specified Spanner spanner_database."""
    connection_info = (instance, database, project, credentials)
    if cls._spanner_connection is not None:
      if connection_info == cls._connection_info:
        return
      cls.hangup()

    client = spanner.Client(project=project, credentials=credentials)
    instance = client.instance(instance)
    cls._spanner_connection = instance.database(database, pool=pool)
    cls._connection_info = connection_info

  @classmethod
  def hangup(cls) -> None:
    cls._spanner_connection = None
    cls._connection_info = None

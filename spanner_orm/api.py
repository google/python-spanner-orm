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
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar

from spanner_orm import error

from google.auth import credentials as auth_credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database as spanner_database
from google.cloud.spanner_v1 import transaction as spanner_transaction
from google.cloud.spanner_v1.proto import type_pb2

_logger = logging.getLogger(__name__)

CallableReturn = TypeVar('CallableReturn')


class SpannerReadApi(abc.ABC):
  """Handles sending read requests to Spanner."""

  @property
  @abc.abstractmethod
  def _connection(self) -> spanner_database.Database:
    raise NotImplementedError

  def run_read_only(self, method: Callable[..., CallableReturn], *args: Any,
                    **kwargs: Any) -> CallableReturn:
    """Wraps read-only queries in a read transaction.

    The callable will be executed with the read transaction (Snapshot from
    the Spanner client library)passed to it as the first argument.

    Args:
      method: The method that will be run in the transaction
      *args: Positional arguments that will be passed to `method`
      **kwargs: Keyword arguments that will be passed to `method`

    Returns:
      The return value from `method` will be returned from this method
    """
    with self._connection.snapshot(multi_use=True) as snapshot:
      return method(snapshot, *args, **kwargs)

  # Read methods
  def find(self, transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str],
           keyset: spanner.KeySet) -> List[Iterable[Any]]:
    """Retrieves rows from the given table based on the provided KeySet.

    Args:
      transaction: The Spanner transaction to execute the request on
      table_name: The Spanner table being queried
      columns: Which columns to retrieve from the Spanner table
      keyset: Contains a list of primary keys that indicates which rows to
        retrieve from the Spanner table

    Returns:
      A list of lists. Each sublist is the set of `columns` requested from
      a row in the Spanner table whose primary key matches one of the
      primary keys in the `keyset`. The order of the values in the sublist
      matches the order of the columns from the `columns` parameter.
    """
    _logger.debug('Find table=%s columns=%s keys=%s', table_name, columns,
                  keyset.keys)
    stream_results = transaction.read(
        table=table_name, columns=columns, keyset=keyset)
    return list(stream_results)

  def sql_query(
      self, transaction: spanner_transaction.Transaction, query: str,
      parameters: Dict[str, Any],
      parameter_types: Dict[str, type_pb2.Type]) -> List[Iterable[Any]]:
    """Executes a given SQL query against the Spanner database.

    This isn't technically read-only, but it's necessary to implement the read-
    only features of the ORM

    Args:
      transaction: The Spanner transaction to execute the request on
      query: The SQL query to run
      parameters: A mapping from the names of the parameters used in the SQL
        query to the value to be substituted in for that parameter
      parameter_types: A mapping from the names of the parameters used in the
        SQL query to the type of the value being substituted in for that
        parameter

    Returns:
      A list of lists. Each sublist is a result row from the SQL query. For
      SELECT queries, the order of values in the sublist matches the order
      of the columns requested from the SELECT clause of the query.
    """
    _logger.debug('Executing SQL:\n%s\n%s\n%s', query, parameters,
                  parameter_types)
    stream_results = transaction.execute_sql(
        query, params=parameters, param_types=parameter_types)
    return list(stream_results)


class SpannerWriteApi(abc.ABC):
  """Handles sending write requests to Spanner."""

  @property
  @abc.abstractmethod
  def _connection(self) -> spanner_database.SpannerDatabase:
    raise NotImplementedError

  def run_write(self, method: Callable[..., CallableReturn], *args: Any,
                **kwargs: Any) -> CallableReturn:
    """Wraps read-write queries in a write transaction.

    The callable will be executed with the write transaction passed to it as
    the first argument. If the transaction aborts (usually because the data
    the write operation depended on has changed since the start of the
    transaction), the callable will continue to be retried until success or
    30 seconds has passed.

    Args:
      method: The method that will be run in the transaction
      *args: Positional arguments that will be passed to `method`
      **kwargs: Keyword arguments that will be passed to `method`

    Returns:
      The return value from `method` will be returned from this method
    """
    return self._connection.run_in_transaction(method, *args, **kwargs)

  def delete(self, transaction: spanner_transaction.Transaction,
             table_name: str, keyset: spanner.KeySet) -> None:
    """Deletes rows from the given table based on the provided KeySet.

    Args:
      transaction: The Spanner transaction to execute the request on
      table_name: The Spanner table being modified
      keyset: Contains a list of primary keys that indicates which rows to
        delete from the Spanner table
    """

    _logger.debug('Delete table=%s keys=%s', table_name, keyset.keys)
    transaction.delete(table=table_name, keyset=keyset)

  def insert(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Adds rows to the given table based on the provided values.

    All non-nullable columns must be specified. Note that if a row is specified
    for which the primary key already exists in the table, an exception will
    be thrown and the insert will be aborted.

    Args:
      transaction: The Spanner transaction to execute the request on
      table_name: The Spanner table being modified
      columns: Which columns to write on the Spanner table
      values: A list of rows to write to the table. The order of the values in
        each sublist must match the order of the columns specified in the
       `columns` parameter.
    """
    _logger.debug('Insert table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.insert(table=table_name, columns=columns, values=values)

  def update(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Updates rows in the given table based on the provided values.

    Note that if a row is specified for which the primary key does not
    exist in the table, an exception will be thrown and the update
    will be aborted.

    Args:
      transaction: The Spanner transaction to execute the request on
      table_name: The Spanner table being modified
      columns: Which columns to write on the Spanner table
      values: A list of rows to write to the table. The order of the values in
        each sublist must match the order of the columns specified in the
        `columns` parameter.
    """
    _logger.debug('Update table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.update(table=table_name, columns=columns, values=values)

  def upsert(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Inserts or updates rows in the given table based on the provided values.

    All non-nullable columns must be specified, similarly to the insert method.
    The presence or absence of data in the table will not cause an exception
    to be thrown, unlike insert or update.

    Args:
      transaction: The Spanner transaction to execute the request on
      table_name: The Spanner table being modified
      columns: Which columns to write on the Spanner table
      values: A list of rows to write to the table. The order of the values in
        each sublist must match the order of the columns specified in the
        `columns` parameter.
    """
    _logger.debug('Upsert table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.insert_or_update(
        table=table_name, columns=columns, values=values)


class SpannerConnection:
  """Class that handles connecting to a Spanner database."""

  def __init__(self,
               instance: str,
               database: str,
               project: Optional[str] = None,
               credentials: Optional[auth_credentials.Credentials] = None,
               pool: Optional[spanner.Pool] = None,
               create_ddl: Optional[Iterable[str]] = None):
    """Connects to the specified Spanner database."""
    client = spanner.Client(project=project, credentials=credentials)
    instance = client.instance(instance)
    self.database = instance.database(
        database, pool=pool, ddl_statements=create_ddl or ())


class SpannerApi(SpannerReadApi, SpannerWriteApi):
  """Class that handles reading from and writing to Spanner tables."""

  def __init__(self, connection: SpannerConnection):
    self._spanner_connection = connection

  @property
  def _connection(self):
    return self._spanner_connection.database


_api = None  # type: Optional[SpannerApi]


def connect(instance: str,
            database: str,
            project: Optional[str] = None,
            credentials: Optional[auth_credentials.Credentials] = None,
            pool: Optional[spanner.Pool] = None) -> SpannerApi:
  """Connects to the Spanner database and sets the global spanner_api."""
  connection = SpannerConnection(
      instance, database, project=project, credentials=credentials, pool=pool)
  return from_connection(connection)


def from_connection(connection: SpannerConnection) -> SpannerApi:
  """Sets the global spanner_api from the provided connection."""
  global _api
  _api = SpannerApi(connection)
  return _api


def hangup() -> None:
  """Clears the global spanner_api."""
  global _api
  _api = None


def spanner_api() -> SpannerApi:
  """Returns the global spanner_api if it has been set."""
  if not _api:
    raise error.SpannerError('Must connect to Spanner before calling APIs')
  return _api

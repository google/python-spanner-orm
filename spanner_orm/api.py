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

  @property
  @abc.abstractmethod
  def _connection(self) -> spanner_database.Database:
    raise NotImplementedError

  def run_read_only(self, method: Callable[..., CallableReturn], *args: Any,
                    **kwargs: Any) -> CallableReturn:
    """Wraps read-only queries in a read transaction."""
    with self._connection.snapshot(multi_use=True) as snapshot:
      return method(snapshot, *args, **kwargs)

  # Read methods
  def find(self, transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str],
           keyset: spanner.KeySet) -> List[Iterable[Any]]:
    """Obtains rows with primary_keys from the given table."""
    _logger.debug('Find table=%s columns=%s keys=%s', table_name, columns,
                  keyset.keys)
    stream_results = transaction.read(
        table=table_name, columns=columns, keyset=keyset)
    return list(stream_results)

  def sql_query(self, transaction: spanner_transaction.Transaction, query: str,
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

  @property
  @abc.abstractmethod
  def _connection(self) -> spanner_database.SpannerDatabase:
    raise NotImplementedError

  def run_write(self, method: Callable[..., CallableReturn], *args: Any,
                **kwargs: Any) -> CallableReturn:
    """Wraps write and read-write queries in a transaction."""
    return self._connection.run_in_transaction(method, *args, **kwargs)

  def delete(self, transaction: spanner_transaction.Transaction,
             table_name: str, keyset: spanner.KeySet) -> None:
    _logger.debug('Delete table=%s keys=%s', table_name, keyset.keys)
    transaction.delete(table=table_name, keyset=keyset)

  def insert(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Add rows to a table."""
    _logger.debug('Insert table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.insert(table=table_name, columns=columns, values=values)

  def update(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Updates rows of a table."""
    _logger.debug('Update table=%s columns=%s values=%s', table_name, columns,
                  values)
    transaction.update(table=table_name, columns=columns, values=values)

  def upsert(self, transaction: spanner_transaction.Transaction,
             table_name: str, columns: Iterable[str],
             values: Iterable[Iterable[Any]]) -> None:
    """Updates existing rows of a table or adds rows if they don't exist."""
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
  connection = SpannerConnection(
      instance, database, project=project, credentials=credentials, pool=pool)
  return from_connection(connection)


def from_connection(connection: SpannerConnection) -> SpannerApi:
  global _api
  _api = SpannerApi(connection)
  return _api


def hangup() -> None:
  global _api
  _api = None


def spanner_api() -> SpannerApi:
  if not _api:
    raise error.SpannerError('Must connect to Spanner before calling APIs')
  return _api

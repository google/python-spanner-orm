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

import abc
from typing import Any, Callable, Iterable, Optional, TypeVar

from spanner_orm import error

from google.auth import credentials as auth_credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database as spanner_database
from google.cloud.spanner_v1 import pool as spanner_pool

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


class SpannerWriteApi(abc.ABC):
  """Handles sending write requests to Spanner."""

  @property
  @abc.abstractmethod
  def _connection(self) -> spanner_database.Database:
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


class SpannerConnection:
  """Class that handles connecting to a Spanner database."""

  def __init__(self,
               instance: str,
               database: str,
               project: Optional[str] = None,
               credentials: Optional[auth_credentials.Credentials] = None,
               pool: Optional[spanner_pool.AbstractSessionPool] = None,
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
            pool: Optional[spanner_pool.AbstractSessionPool] = None
           ) -> SpannerApi:
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

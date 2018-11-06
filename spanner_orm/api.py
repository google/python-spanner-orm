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
"""Class that handles API calls to Spanner."""

import abc
from google.cloud import spanner


class SpannerReadApi(abc.ABC):
  """Handles sending read requests to Spanner"""

  @classmethod
  @abc.abstractmethod
  def _connection(cls):
    raise NotImplementedError

  @classmethod
  def run_read_only(cls, method, *args, **kwargs):
    """Wraps read-only queries in a read transaction."""
    with cls._connection().snapshot() as snapshot:
      return method(snapshot, *args, **kwargs)

  # Read methods
  @staticmethod
  def find(transaction, table_name, columns, keyset):
    """Obtains rows with primary_keys from the given table."""
    stream_results = transaction.read(
        table=table_name, columns=columns, keyset=keyset)
    return list(stream_results)

  @staticmethod
  def sql_query(transaction, query, parameters, parameter_types):
    """Runs a read only SQL query."""
    stream_results = transaction.execute_sql(
        query, params=parameters, param_types=parameter_types)
    return list(stream_results)


class SpannerWriteApi(abc.ABC):
  """Handles sending write requests to Spanner."""

  @classmethod
  @abc.abstractmethod
  def _connection(cls):
    raise NotImplementedError

  @classmethod
  def run_write(cls, *args, **kwargs):
    """Wraps write and read-write queries in a transaction."""
    return cls._connection().run_in_transaction(*args, **kwargs)

  # Write methods
  @staticmethod
  def insert(transaction, table_name, columns, values):
    """Add rows to a table."""
    transaction.insert(table=table_name, columns=columns, values=values)

  @staticmethod
  def update(transaction, table_name, columns, values):
    """Updates rows of a table."""
    transaction.update(table=table_name, columns=columns, values=values)

  @staticmethod
  def upsert(transaction, table_name, columns, values):
    """Updates existing rows of a table or adds rows if they don't exist."""
    transaction.insert_or_update(
        table=table_name, columns=columns, values=values)


class SpannerApi(SpannerReadApi, SpannerWriteApi):
  """Class that handles reading from and writing to Spanner tables."""

  _connection_info = None
  _spanner_connection = None

  @classmethod
  def _connection(cls):
    assert cls._spanner_connection is not None, 'Not connected to Spanner'
    return cls._spanner_connection

  # Spanner connection methods
  @classmethod
  def connect(cls, project, instance, database, credentials=None):
    """Connects to the specified Spanner database."""
    connection_info = (project, instance, database, credentials)
    if cls._spanner_connection is not None:
      if connection_info == cls._connection_info:
        return
      cls.hangup()

    client = spanner.Client(project=project, credentials=credentials)
    instance = client.instance(instance)
    cls._spanner_connection = instance.database(database)
    cls._connection_info = connection_info

  @classmethod
  def hangup(cls):
    cls._spanner_connection = None
    cls._connection_info = None

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

# python3
"""Class that interacts with spanner database."""

from abc import ABC
from abc import abstractmethod
from google.cloud import spanner


class TableReadApi(ABC):
  """Handles read from table interactions with Spanner"""

  @classmethod
  @abstractmethod
  def _database_connection(cls):
    raise NotImplementedError

  @classmethod
  def run_read_only(cls, method, *args, **kwargs):
    """Executes the provided callback method for read-only queries."""
    with cls._database_connection().snapshot() as snapshot:
      return method(snapshot, *args, **kwargs)

  # Read methods
  @staticmethod
  def find(transaction, table_name, columns, keyset):
    """Obtains rows with primary_keys from the given table."""
    stream_results = transaction.read(
        table=table_name, columns=columns, keyset=keyset)
    results = list(stream_results)

    # If number of primary keys is specified, then number of results
    # should be less than or equal to number of keys
    if not keyset.all_ and len(results) > len(keyset.keys):
      error = 'ERROR: primary_keys "{}" returns extra rows for table "{}"'
      raise BaseException(error.format(str(keyset), table_name))

    return results

  @staticmethod
  def sql_query(transaction, query, parameters, parameter_types):
    """Runs a read only SQL query."""
    stream_results = transaction.execute_sql(
        query, params=parameters, param_types=parameter_types)
    return list(stream_results)


class TableWriteApi(ABC):
  """Handles write to table interactions with Spanner"""

  @classmethod
  @abstractmethod
  def _database_connection(cls):
    raise NotImplementedError

  @classmethod
  def run_write(cls, *args, **kwargs):
    """Executes the provided callback method in a transaction."""
    return cls._database_connection().run_in_transaction(*args, **kwargs)

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
    """Updates row if primary key already exists, otherwise, creates a row."""
    transaction.insert_or_update(
        table=table_name, columns=columns, values=values)


class DatabaseApi(TableReadApi, TableWriteApi):
  """Class that handles reading from and writing to Spanner tables"""

  _connection = None
  _connection_info = None

  @classmethod
  def _database_connection(cls):
    assert cls._connection is not None
    return cls._connection

  # Spanner connection methods
  @classmethod
  def connect(cls, project, instance, database):
    """Connects to the specified Spanner database"""
    connection_info = (project, instance, database)
    if cls._connection is not None:
      if connection_info == cls._connection_info:
        return
      cls.hangup()

    client = spanner.Client(project=project)
    instance = client.instance(instance)
    cls._connection = instance.database(database)
    cls._connection_info = connection_info

  @classmethod
  def hangup(cls):
    cls._connection = None
    cls._connection_info = None

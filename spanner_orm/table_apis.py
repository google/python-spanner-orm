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
"""Table-level API lambdas for Spanner transactions."""

import logging
from typing import Any, Dict, Iterable, List

from google.cloud import spanner
from google.cloud.spanner_v1 import transaction as spanner_transaction
from google.cloud.spanner_v1.proto import type_pb2

_logger = logging.getLogger(__name__)


# Read methods
def find(transaction: spanner_transaction.Transaction, table_name: str,
         columns: Iterable[str], keyset: spanner.KeySet) -> List[Iterable[Any]]:
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


def sql_query(transaction: spanner_transaction.Transaction, query: str,
              parameters: Dict[str, Any],
              parameter_types: Dict[str, type_pb2.Type]) -> List[Iterable[Any]]:
  """Executes a given SQL query against the Spanner database.

  This isn't technically read-only, but it's necessary to implement the read-
  only features of the ORM

  Args:
    transaction: The Spanner transaction to execute the request on
    query: The SQL query to run
    parameters: A mapping from the names of the parameters used in the SQL query
      to the value to be substituted in for that parameter
    parameter_types: A mapping from the names of the parameters used in the SQL
      query to the type of the value being substituted in for that parameter

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


def delete(transaction: spanner_transaction.Transaction, table_name: str,
           keyset: spanner.KeySet) -> None:
  """Deletes rows from the given table based on the provided KeySet.

  Args:
    transaction: The Spanner transaction to execute the request on
    table_name: The Spanner table being modified
    keyset: Contains a list of primary keys that indicates which rows to delete
      from the Spanner table
  """

  _logger.debug('Delete table=%s keys=%s', table_name, keyset.keys)
  transaction.delete(table=table_name, keyset=keyset)


def insert(transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
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


def update(transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
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


def upsert(transaction: spanner_transaction.Transaction, table_name: str,
           columns: Iterable[str], values: Iterable[Iterable[Any]]) -> None:
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
  transaction.insert_or_update(table=table_name, columns=columns, values=values)

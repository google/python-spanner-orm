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
"""Interacts with the Spanner database to read and manage table schemas."""

from spanner_orm.api import TableReadApi

from google.cloud import spanner


class DatabaseAdminApi(TableReadApi):
  """Manages table schema information on Spanner."""

  _connection = None
  _connection_info = None

  @classmethod
  def connect(cls, project, instance, database, create_ddl=None):
    """Connects to the specified database, optionally creating tables."""
    connection_info = (project, instance, database)
    if cls._connection is not None and connection_info == cls._connection_info:
      return

    client = spanner.Client(project=project)
    instance = client.instance(instance)

    if create_ddl is not None:
      cls._connection = instance.database(database, ddl_statements=create_ddl)
      operation = cls._connection.create()
      operation.result()
    else:
      cls._connection = instance.database(database)

    cls._connection_info = connection_info

  @classmethod
  def _database_connection(cls):
    assert cls._connection is not None
    return cls._connection

  @classmethod
  def hangup(cls):
    cls._connection = None
    cls._connection_info = None

  @classmethod
  def update_schema(cls, change):
    operation = cls._database_connection().update_ddl([change])
    operation.result()

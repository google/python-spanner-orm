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
"""Class that handles API calls to Spanner that deal with table metadata."""

from spanner_orm import api
from spanner_orm import error

from google.cloud import spanner


class SpannerAdminApi(api.SpannerReadApi):
  """Manages table schema information on Spanner."""

  _connection_info = None
  _spanner_connection = None

  @classmethod
  def connect(cls,
              project,
              instance,
              database,
              credentials=None,
              create_ddl=None):
    """Connects to the specified database, optionally creating tables."""
    connection_info = (project, instance, database, credentials)
    if cls._spanner_connection is not None:
      if connection_info == cls._connection_info:
        return
      cls.hangup()

    client = spanner.Client(project=project, credentials=credentials)
    instance = client.instance(instance)

    if create_ddl is not None:
      cls._spanner_connection = instance.database(
          database, ddl_statements=create_ddl)
      operation = cls._spanner_connection.create()
      operation.result()
    else:
      cls._spanner_connection = instance.database(database)

    cls._connection_info = connection_info

  @classmethod
  def _connection(cls):
    if not cls._spanner_connection:
      raise error.SpannerError('Not connected to spanner')
    return cls._spanner_connection

  @classmethod
  def hangup(cls):
    cls._spanner_connection = None
    cls._connection_info = None

  @classmethod
  def update_schema(cls, change):
    operation = cls._connection().update_ddl([change])
    operation.result()

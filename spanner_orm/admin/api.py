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
"""Class that handles API calls to Spanner that deal with table metadata."""

from __future__ import annotations

from typing import Iterable, Optional
from spanner_orm import api
from spanner_orm import error

from google.auth import credentials as auth_credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database as spanner_database


class SpannerAdminApi(api.SpannerReadApi):
  """Manages table schema information on Spanner."""

  _connection_info = None
  _spanner_connection = None

  @classmethod
  def connect(cls,
              instance: str,
              database: str,
              project: Optional[str] = None,
              credentials: Optional[auth_credentials.Credentials] = None,
              create_ddl: Optional[Iterable[str]] = None):
    """Connects to the specified database, optionally creating tables."""
    connection_info = (instance, database, project, credentials)
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
  def _connection(cls) -> spanner_database.SpannerDatabase:
    if not cls._spanner_connection:
      raise error.SpannerError('Not connected to Spanner')
    return cls._spanner_connection

  @classmethod
  def drop_database(cls) -> None:
    cls._connection().drop()
    cls.hangup()

  @classmethod
  def hangup(cls) -> None:
    cls._spanner_connection = None
    cls._connection_info = None

  @classmethod
  def update_schema(cls, change: str) -> None:
    operation = cls._connection().update_ddl([change])
    operation.result()

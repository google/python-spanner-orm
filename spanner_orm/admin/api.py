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

from typing import Iterable, Optional
from spanner_orm import api
from spanner_orm import error

from google.auth import credentials as auth_credentials
from google.cloud.spanner_v1 import database as spanner_database
from google.cloud.spanner_v1 import pool as spanner_pool


class SpannerAdminApi(api.SpannerReadApi, api.SpannerWriteApi):
  """Manages table schema information on Spanner."""

  def __init__(self, connection: api.SpannerConnection):
    self._spanner_connection = connection

  @property
  def _connection(self) -> spanner_database.Database:
    return self._spanner_connection.database

  def create_database(self) -> None:
    operation = self._connection.create()
    operation.result()

  def drop_database(self) -> None:
    self._connection.drop()

  def update_schema(self, change: str) -> None:
    operation = self._connection.update_ddl([change])
    operation.result()


_admin_api = None


def connect(instance: str,
            database: str,
            project: Optional[str] = None,
            credentials: Optional[auth_credentials.Credentials] = None,
            pool: Optional[spanner_pool.AbstractSessionPool] = None,
            create_ddl: Optional[Iterable[str]] = None) -> SpannerAdminApi:
  """Connects the global Spanner admin API to a Spanner database."""
  connection = api.SpannerConnection(
      instance,
      database,
      project=project,
      credentials=credentials,
      pool=pool,
      create_ddl=create_ddl)
  return from_connection(connection)


def from_connection(connection: api.SpannerConnection) -> SpannerAdminApi:
  global _admin_api
  _admin_api = SpannerAdminApi(connection)
  return _admin_api


def hangup() -> None:
  global _admin_api
  _admin_api = None


def spanner_admin_api() -> SpannerAdminApi:
  if not _admin_api:
    raise error.SpannerError('Must connect to Spanner before calling APIs')
  return _admin_api

# Copyright 2020 Google LLC
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
"""Superclass and helpers for tests that use the spanner emulator."""

import os
import unittest
import uuid

import spanner_orm

from google.cloud.spanner_v1 import client
from google.cloud.spanner_v1 import database
from google.cloud.spanner_v1 import instance
from spanner_orm.testlib.spanner_emulator import emulator


def _make_emulator_spanner_orm_connection(
    db: database.Database, inst: instance.Instance,
    spanner_client: client.Client) -> spanner_orm.SpannerConnection:
  """Returns an spanner_orm.connection to a spanner database.

  Args:
    db: database that already exists in spanner
    inst: instance that already exists in spanner
    spanner_client: client with access to the database and instance provided
  """
  # project/project-name -> project-name.
  project_name = spanner_client.project_name.split('/')[1]
  return spanner_orm.SpannerConnection(
      inst.instance_id,
      db.database_id,
      project=project_name,
      credentials=spanner_client.credentials)


def _get_instance(spanner_client: client.Client) -> instance.Instance:
  """Returns a spanner instance from the client.

  First, checks if there is an existing instance that can be re-used, returning
  it if one exists. Otherwise, create a new instance, waits for it to be created
  and then returns it.

  Args:
    spanner_client: An initialized spanner client.
  """
  existing_instances_pb = list(spanner_client.list_instances())
  if existing_instances_pb:
    return instance.Instance.from_pb(existing_instances_pb[0], spanner_client)

  # The emulator has one default config.
  config = list(spanner_client.list_instance_configs())[0]
  inst = spanner_client.instance(
      'spanner-instance-name', configuration_name=config.name)
  inst.create().result()
  return inst


def _migrate_database_at_connection(connection: spanner_orm.SpannerConnection,
                                    migrations_dir: str) -> None:
  """Applies the migrations to the provided connection."""
  spanner_orm.from_connection(connection)
  executor = spanner_orm.MigrationExecutor(connection, basedir=migrations_dir)
  executor.migrate()


def _database_id() -> str:
  """Returns a new database ID that's unlikely to conflict with any other."""
  random_string = str(uuid.uuid4()).split('-')[0]
  return 'spanner-db-' + random_string


class TestCase(unittest.TestCase):
  """Sets up a spanner emulator database for each test case.

  Any test class that subclasses this class will have a spanner database
  setup for it. That database is empty by default; most subclasses will want to
  call run_orm_migrations() in their setUp() method.

  Attributes:
    spanner_emulator_client: Client, for use by non-ORM tests.
    spanner_emulator_instance: Instance, for use by non-ORM tests.
    spanner_emulator_database: Database, for use by non-ORM tests.
  """

  _spanner_emulator: emulator.Emulator

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._spanner_emulator = emulator.Emulator()

  def setUp(self):
    super().setUp()
    self.spanner_emulator_client = self._spanner_emulator.get_client()
    self.spanner_emulator_instance = _get_instance(self.spanner_emulator_client)
    self.spanner_emulator_database = self.spanner_emulator_instance.database(
        _database_id())
    self.spanner_emulator_database.create().result()

  @classmethod
  def tearDownClass(cls):
    cls._spanner_emulator.stop()
    super().tearDownClass()

  def run_orm_migrations(self, migrations_folder: str) -> None:
    """Runs ORM migrations in the given directory and connects the ORM."""
    connection = _make_emulator_spanner_orm_connection(
        self.spanner_emulator_database, self.spanner_emulator_instance,
        self.spanner_emulator_client)
    _migrate_database_at_connection(connection, migrations_folder)
    # spanner_orm closes the connection to Spanner after migrating so we need to
    # reconnect before making other Spanner calls.
    spanner_orm.from_connection(connection)
    spanner_orm.from_admin_connection(connection)

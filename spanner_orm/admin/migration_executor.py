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
"""Handles execution of migrations."""

import datetime
import logging
from typing import Iterable, List, Dict, Optional

from spanner_orm import api
from spanner_orm import error
from spanner_orm.admin import api as admin_api
from spanner_orm.admin import metadata
from spanner_orm.admin import migration
from spanner_orm.admin import migration_manager
from spanner_orm.admin import migration_status
from spanner_orm.admin import update

_logger = logging.getLogger(__name__)


class MigrationExecutor:
  """Handles execution of migrations."""

  def __init__(self,
               connection: api.SpannerConnection,
               basedir: Optional[str] = None):
    self._manager = migration_manager.MigrationManager(basedir)
    self._migration_status_map = None
    self._connection = connection

  def migrated(self, migration_id: str) -> bool:
    if migration_id is None:
      return True
    return self._migration_status().get(migration_id, False)

  def migrations(self) -> List[migration.Migration]:
    return self._manager.migrations

  def migrate(self, target_migration: Optional[str] = None) -> None:
    """Executes unmigrated migrations on the curent database.

    Note: SpannerAdminApi connection is modified as a result of calling
    this method. Other connections to SpannerAdminApi in the same process
    may be affected.

    Args:
      target_migration: If present, stop migrations after the target is
        executed. If None (default), executes all unmigrated migrations
    """
    self._connect()
    self._validate_migrations()
    # Filter to unmigrated migrations
    migrations = self._filter_migrations(self.migrations(), False,
                                         target_migration)
    for migration_ in migrations:
      _logger.info('Processing migration %s', migration_.migration_id)
      schema_update = migration_.upgrade()
      if not isinstance(schema_update, update.SchemaUpdate):
        raise error.SpannerError(
            'Migration {} did not return a SchemaUpdate'.format(
                migration_.migration_id))
      schema_update.execute()

      self._update_status(migration_.migration_id, True)
    self._hangup()

  def show_migrations(self) -> None:
    """Prints information about all migrations.
    """
    self._connect()
    self._validate_migrations()

    for migration_ in reversed(self.migrations()):
      migrated = self.migrated(migration_.migration_id)
      print('[{}] {}, {}'.format('X' if migrated else ' ', migration_.migration_id, migration_.description))

    self._hangup()

  def rollback(self, target_migration: str) -> None:
    """Rolls back migrated migrations on the curent database.

    Note: SpannerAdminApi connection is modified as a result of calling
    this method. Other connections to SpannerAdminApi in the same process
    may be affected.

    Args:
      target_migration: Stop rolling back migrations after this migration is
        rolled back. Must be present in list of migrated migrations.
    """
    if not target_migration:
      raise error.SpannerError('Must specify a migration to roll back')

    self._connect()
    self._validate_migrations()
    # Filter to migrated migrations from most recently applied
    migrations = self._filter_migrations(
        reversed(self.migrations()), True, target_migration)
    for migration_ in migrations:
      _logger.info('Processing migration %s', migration_.migration_id)
      schema_update = migration_.downgrade()
      if not isinstance(schema_update, update.SchemaUpdate):
        raise error.SpannerError(
            'Migration {} did not return a SchemaUpdate'.format(
                migration_.migration_id))
      schema_update.execute()

      self._update_status(migration_.migration_id, False)
    self._hangup()

  def _connect(self) -> None:
    api_connection = admin_api.from_connection(self._connection)
    if not self._connection.database.exists():
      api_connection.create_database()

  def _hangup(self) -> None:
    admin_api.hangup()

  def _filter_migrations(
      self, migrations: Iterable[migration.Migration], migrated: bool,
      last_migration: Optional[str] = None) -> List[migration.Migration]:
    """Filters the list of migrations according to the desired conditions.

    Args:
      migrations: List of migrations to filter
      migrated: Only add migrations whose migration status matches this flag
      last_migration: Stop adding migrations to the list after this one is found

    Returns:
      List of filtered migrations
    """
    filtered = []
    last_migration_found = False
    for migration_ in migrations:
      if self.migrated(migration_.migration_id) == migrated:
        filtered.append(migration_)

        if last_migration and migration_.migration_id == last_migration:
          last_migration_found = True
          break

    if last_migration and not last_migration_found:
      raise error.SpannerError(
          '{} already has desired status or does not exist'.format(
              last_migration))
    return filtered

  def _migration_status(self) -> Dict[str, bool]:
    """Gathers from Spanner which migrations have been executed."""
    if self._migration_status_map is None:
      model_from_db = metadata.SpannerMetadata.model(
          migration_status.MigrationStatus.table)
      if not model_from_db:
        update.CreateTable(migration_status.MigrationStatus).execute()
      self._migration_status_map = {
          migration_.id: migration_.migrated
          for migration_ in migration_status.MigrationStatus.all()
      }

    return self._migration_status_map

  def _update_status(self, migration_id: str, new_status: bool) -> None:
    """Updates migration status in the database for the given migration."""
    new_model = migration_status.MigrationStatus({
        'id': migration_id,
        'migrated': new_status,
        'update_time': datetime.datetime.utcnow(),
    })
    migration_status.MigrationStatus.save_batch(
        None, [new_model], force_write=True)
    self._migration_status()[migration_id] = new_status

  def _validate_migrations(self) -> None:
    """Validates the migration status of all migrations makes sense."""
    migrations = self.migrations()
    if not migrations:
      return

    first = migrations[0]
    if not self.migrated(first.prev_migration_id):
      raise error.SpannerError(
          'First migration {} depends on unmigrated migration {}'.format(
              first.migration_id, first.prev_migration_id))

    for migration_ in migrations:
      if (self.migrated(migration_.migration_id) and
          not self.migrated(migration_.prev_migration_id)):
        raise error.SpannerError(
            'Migrated migration {} depends on an unmigrated migration'.format(
                migration_.migration_id))

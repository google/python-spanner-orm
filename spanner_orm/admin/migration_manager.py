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
"""Handles reading and writing of migration files."""

from __future__ import annotations


import datetime
import importlib
import os
import re
import string
from typing import Any, Iterable, List, Optional
import uuid

from spanner_orm import error


class MigrationManager:
  """Handles reading and writing of migration files."""
  DEFAULT_DIRECTORY = 'migrations'

  def __init__(self, basedir: Optional[str] = None):
    self.basedir = basedir or self.DEFAULT_DIRECTORY
    self._migrations = None

    if not os.path.exists(self.basedir):
      os.makedirs(self.basedir)

  def generate(self, migration_name: str) -> str:
    """Creates a new migration that is the last migration to be executed."""
    migration_id = uuid.uuid4().hex[-12:]
    prev_id = self.migrations[-1].migration_id if self.migrations else None
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    skeleton_directory = os.path.dirname(os.path.abspath(__file__))
    skeleton_file = os.path.join(skeleton_directory, 'migration.skel')
    with open(skeleton_file, 'r') as skeleton:
      migration_skeleton = string.Template(skeleton.read())
    migration_content = migration_skeleton.substitute(
        migration_name=migration_name,
        migration_id=repr(migration_id),
        prev_migration_id=repr(prev_id),
        current_date=now)

    filename = '{name}_{migration_id}.py'.format(
        name=re.sub(r'\W', '_', migration_name), migration_id=migration_id)
    filepath = os.path.join(self.basedir, filename)
    with open(filepath, 'w') as f:
      f.write(migration_content)
    return filepath

  @property
  def migrations(self) -> Any:
    """Loads and orders all migrations in the base dir."""
    if self._migrations is None:
      unordered_migrations = self._all_migrations()
      self._migrations = self._order_migrations(unordered_migrations)
    return self._migrations

  def _migration_from_file(self, filename: str) -> Any:
    """Loads a single migration from the given filename in the base dir."""
    module_name = re.sub(r'\W', '_', filename)
    path = os.path.join(self.basedir, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, 'migration_id'):
      raise error.SpannerError('{} has no migration id'.format(path))
    return module

  def _all_migrations(self) -> List[Any]:
    """Loads all migrations from the base dir."""
    migrations = []
    for filename in os.listdir(self.basedir):
      _, ext = os.path.splitext(filename)
      if ext == '.py':
        migrations.append(self._migration_from_file(filename))
    return migrations

  def _order_migrations(self, migrations: Iterable[Any]) -> List[Any]:
    """Returns list of migrations in the order they have to be applied."""
    if not migrations:
      return []

    id_map = {migration.migration_id: migration for migration in migrations}
    start_migration = None
    for migration_id, migration in id_map.items():
      if migration.prev_migration_id and migration.prev_migration_id in id_map:
        current = id_map[migration.prev_migration_id]
        if hasattr(current, 'next'):
          raise error.SpannerError(
              '{name} has unclear successor migration'.format(
                  name=current.migration_id))
        current.next = migration_id
      else:
        if start_migration:
          raise error.SpannerError(
              'Multiple migrations have no valid previous migration')
        start_migration = migration_id

    if not start_migration:
      raise error.SpannerError('No valid migration to start from')

    migration_order = []
    while start_migration:
      current = id_map[start_migration]
      migration_order.append(current)
      start_migration = getattr(current, 'next', None)

    if len(migration_order) != len(id_map):
      raise error.SpannerError('{} has no successor migration'.format(
          migration_order[-1].migration_id))
    return migration_order

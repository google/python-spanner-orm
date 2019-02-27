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
import logging
import os
import unittest
from unittest import mock

from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import migration_manager


class TestMigration(object):

  def __init__(self, migration_id, prev_migration):
    self._id = migration_id
    self._prev = prev_migration

  @property
  def migration_id(self):
    return self._id

  @property
  def prev_migration(self):
    return self._prev

  def upgrade(self):
    pass

  def downgrade(self):
    pass


class MigrationsTest(unittest.TestCase):
  TEST_DIR = os.path.dirname(__file__)
  TEST_MIGRATIONS_DIR = os.path.join(TEST_DIR, 'migrations')

  def test_retrieve(self):
    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    migrations = manager.migrations
    self.assertEqual(len(migrations), 3)
    self.assertEqual(migrations[2].prev_migration, migrations[1].migration_id)
    self.assertEqual(migrations[1].prev_migration, migrations[0].migration_id)

  def test_generate(self):
    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    try:
      path = manager.generate('test migration')
      migration = manager._migration_from_file(path)
      self.assertIsNotNone(migration.migration_id)
      self.assertIsNotNone(migration.prev_migration)
      self.assertIsNotNone(migration.upgrade)
      self.assertIsNotNone(migration.downgrade)
    except Exception as ex:
      raise ex
    finally:
      os.remove(path)

  def test_order_migrations(self):
    first = TestMigration('1', None)
    second = TestMigration('2', '1')
    third = TestMigration('3', '2')
    migrations = [third, first, second]
    expected_order = [first, second, third]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    self.assertEqual(manager._order_migrations(migrations), expected_order)

  def test_order_migrations_with_no_none(self):
    first = TestMigration('2', '1')
    second = TestMigration('3', '2')
    third = TestMigration('4', '3')
    migrations = [third, first, second]
    expected_order = [first, second, third]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    self.assertEqual(manager._order_migrations(migrations), expected_order)

  def test_order_migrations_error_on_unclear_successor(self):
    first = TestMigration('1', None)
    second = TestMigration('2', '1')
    third = TestMigration('3', '1')
    migrations = [third, first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'unclear successor'):
      manager._order_migrations(migrations)

  def test_order_migrations_error_on_unclear_start_migration(self):
    first = TestMigration('1', None)
    second = TestMigration('3', '2')
    migrations = [first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'no valid previous'):
      manager._order_migrations(migrations)

  def test_order_migrations_error_on_circular_dependency(self):
    first = TestMigration('1', '3')
    second = TestMigration('2', '1')
    third = TestMigration('3', '2')
    migrations = [third, first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'No valid migration'):
      manager._order_migrations(migrations)


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

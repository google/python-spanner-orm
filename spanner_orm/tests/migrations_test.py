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
from io import StringIO
import logging
import os
import shutil
import stat
import tempfile
import unittest
from unittest import mock

from spanner_orm import error
from spanner_orm.admin import migration
from spanner_orm.admin import migration_executor
from spanner_orm.admin import migration_manager
from spanner_orm.admin import update


class MigrationsTest(unittest.TestCase):
  TEST_DIR = tempfile.mkdtemp()
  TEST_MIGRATIONS_DIR = os.path.join(TEST_DIR, 'migrations')

  def test_retrieve(self):
    testdata_filename = os.path.join(os.path.dirname(__file__),
                                     'migrations')
    manager = migration_manager.MigrationManager(testdata_filename)
    migrations = manager.migrations
    self.assertEqual(len(migrations), 3)
    self.assertEqual(migrations[2].prev_migration_id,
                     migrations[1].migration_id)
    self.assertEqual(migrations[1].prev_migration_id,
                     migrations[0].migration_id)

  def test_generate(self):
    testdata_filename = os.path.join(os.path.dirname(__file__),
                                     'migrations')
    shutil.rmtree(self.TEST_MIGRATIONS_DIR)
    shutil.copytree(testdata_filename, self.TEST_MIGRATIONS_DIR)
    os.chmod(self.TEST_MIGRATIONS_DIR,
             stat.S_IRWXO | stat.S_IRWXU)
    for f in os.listdir(self.TEST_MIGRATIONS_DIR):
      file_path = os.path.join(self.TEST_MIGRATIONS_DIR, f)
      if not os.path.isdir(file_path):
        os.chmod(file_path,
                 stat.S_IROTH | stat.S_IWOTH | stat.S_IRUSR | stat.S_IWUSR)
    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    path = manager.generate('test migration')
    try:
      migration_ = manager._migration_from_file(path)
      self.assertIsNotNone(migration_.migration_id)
      self.assertIsNotNone(migration_.prev_migration_id)
      self.assertIsInstance(migration_.upgrade(), update.NoUpdate)
      self.assertIsInstance(migration_.downgrade(), update.NoUpdate)
    finally:
      shutil.rmtree(self.TEST_MIGRATIONS_DIR)

  def test_order_migrations(self):
    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    migrations = [third, first, second]
    expected_order = [first, second, third]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    self.assertEqual(manager._order_migrations(migrations), expected_order)

  def test_order_migrations_with_no_none(self):
    first = migration.Migration('2', '1', '2')
    second = migration.Migration('3', '2', '3')
    third = migration.Migration('4', '3', '4')
    migrations = [third, first, second]
    expected_order = [first, second, third]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    self.assertEqual(manager._order_migrations(migrations), expected_order)

  def test_order_migrations_error_on_unclear_successor(self):
    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '1', '3')
    migrations = [third, first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'unclear successor'):
      manager._order_migrations(migrations)

  def test_order_migrations_error_on_unclear_start_migration(self):
    first = migration.Migration('1', None, '1')
    second = migration.Migration('3', '2', '3')
    migrations = [first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'no valid previous'):
      manager._order_migrations(migrations)

  def test_order_migrations_error_on_circular_dependency(self):
    first = migration.Migration('1', '3', '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    migrations = [third, first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'No valid migration'):
      manager._order_migrations(migrations)

  def test_order_migrations_error_on_no_successor(self):
    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '3', '2')
    third = migration.Migration('3', '2', '3')
    migrations = [third, first, second]

    manager = migration_manager.MigrationManager(self.TEST_MIGRATIONS_DIR)
    with self.assertRaisesRegex(error.SpannerError, 'no successor'):
      manager._order_migrations(migrations)

  def test_filter_migrations(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    migrations = [first, second, third]

    migrated = {'1': True, '2': False, '3': False}
    with mock.patch.object(executor, '_migration_status_map', migrated):
      filtered = executor._filter_migrations(migrations, False, None)
      self.assertEqual(filtered, [second, third])

      filtered = executor._filter_migrations(migrations, False, '2')
      self.assertEqual(filtered, [second])

      filtered = executor._filter_migrations(reversed(migrations), True, '1')
      self.assertEqual(filtered, [first])

  def test_filter_migrations_error_on_bad_last_migration(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    migrations = [first, second, third]

    migrated = {'1': True, '2': False, '3': False}
    with mock.patch.object(executor, '_migration_status_map', migrated):
      with self.assertRaises(error.SpannerError):
        executor._filter_migrations(migrations, False, '1')

      with self.assertRaises(error.SpannerError):
        executor._filter_migrations(migrations, False, '4')

  def test_validate_migrations(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first, second, third]

      migrated = {'1': True, '2': False, '3': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        executor._validate_migrations()

      migrated = {'1': False, '2': False, '3': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        executor._validate_migrations()

  def test_validate_migrations_error_on_unmigrated_after_migrated(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first, second, third]

      migrated = {'1': False, '2': True, '3': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        with self.assertRaises(error.SpannerError):
          executor._validate_migrations()

      migrated = {'1': False, '2': False, '3': True}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        with self.assertRaises(error.SpannerError):
          executor._validate_migrations()

  def test_validate_migrations_error_on_unmigrated_first(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('2', '1', '2')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first]

      migrated = {'1': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        with self.assertRaises(error.SpannerError):
          executor._validate_migrations()

      migrated = {}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        with self.assertRaises(error.SpannerError):
          executor._validate_migrations()

  def test_migrate(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
      connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first, second, third]
      migrated = {'1': True, '2': False, '3': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        executor.migrate()
        self.assertEqual(migrated, {'1': True, '2': True, '3': True})

  def test_show_migrations(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
      connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('abcdef', None, '1')
    second = migration.Migration('012345', 'abcdef', '2')
    third = migration.Migration('6abcde', '012345', '3')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first, second, third]
      migrated = {'abcdef': True, '012345': False, '6abcde': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        with mock.patch('sys.stdout', new_callable=StringIO) as mock_stdout:
          executor.show_migrations()
          self.assertEqual("[ ] 6abcde, 3\n[ ] 012345, 2\n[X] abcdef, 1\n", mock_stdout.getvalue())

  def test_rollback(self):
    connection = mock.Mock()
    executor = migration_executor.MigrationExecutor(
        connection, self.TEST_MIGRATIONS_DIR)

    first = migration.Migration('1', None, '1')
    second = migration.Migration('2', '1', '2')
    third = migration.Migration('3', '2', '3')
    with mock.patch.object(executor, 'migrations') as migrations:
      migrations.return_value = [first, second, third]
      migrated = {'1': True, '2': False, '3': False}
      with mock.patch.object(executor, '_migration_status_map', migrated):
        executor.rollback('1')
        self.assertEqual(migrated, {'1': False, '2': False, '3': False})

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()
    shutil.rmtree(MigrationsTest.TEST_DIR)

if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()

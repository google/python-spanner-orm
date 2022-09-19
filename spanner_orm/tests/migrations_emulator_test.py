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
import datetime
import logging
import os
import textwrap
from typing import Iterable, Type
import unittest

from absl.testing import absltest
from absl.testing import parameterized
import spanner_orm
from spanner_orm.admin import metadata
from spanner_orm.tests import models
from spanner_orm.testlib.spanner_emulator import testlib as spanner_emulator_testlib

from google.api_core import exceptions as google_api_exceptions


class MigrationsEmulatorTest(spanner_emulator_testlib.TestCase):
  """Basic tests using generic migrations."""

  TEST_MIGRATIONS_DIR = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      'migrations_for_emulator_test',
  )

  def setUp(self):
    super().setUp()
    self.run_orm_migrations(self.TEST_MIGRATIONS_DIR)

  def test_basic(self):
    models.SmallTestModel({'key': 'key', 'value_1': 'value'}).save()
    self.assertEqual(
        [x.values for x in models.SmallTestModel.all()],
        [{
            'key': 'key',
            'value_1': 'value',
            'value_2': None,
        }],
    )

  def test_error_with_missing_referencing_key(self):
    with self.assertRaisesRegex(
        google_api_exceptions.FailedPrecondition,
        'Cannot find referenced key',
    ):
      models.ForeignKeyTestModel({
          'referencing_key_1': 'key',
          'referencing_key_2': 'key',
          'referencing_key_3': 42,
          'value': 'value'
      }).save()

  def test_key(self):
    models.SmallTestModel({'key': 'key', 'value_1': 'value'}).save()
    models.UnittestModel({
        'string': 'string',
        'int_': 42,
        'float_': 4.2,
        'bytes_': b'A1A1',
        'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
    }).save()
    models.ForeignKeyTestModel({
        'referencing_key_1': 'key',
        'referencing_key_2': 'string',
        'referencing_key_3': 42,
        'value': 'value'
    }).save()


class SpecificMigrationsEmulatorTest(
    parameterized.TestCase,
    spanner_emulator_testlib.TestCase,
):
  """Tests of specific migrations."""

  def setUp(self):
    super().setUp()
    self._migrations_dir = self.create_tempdir()
    self._migration_index = None

  def _append_migrations(self, *migrations: str) -> None:
    """Appends migrations to the sequence of migrations in self._migrations_dir.

    Args:
      *migrations: Each string is the python code to define a single upgrade()
        function. Leading indentation is stripped and migration boilerplate is
        added.
    """
    for migration in migrations:
      if self._migration_index is None:
        prev_migration_id = None
        self._migration_index = 0
      else:
        prev_migration_id = str(self._migration_index)
        self._migration_index += 1
      migration_id = str(self._migration_index)
      self._migrations_dir.create_file(
          f'migration_{migration_id}.py',
          '\n'.join((
              'import spanner_orm',
              f'migration_id = {migration_id!r}',
              f'prev_migration_id = {prev_migration_id!r}',
              textwrap.dedent(migration),
              'def downgrade(): raise NotImplementedError()',
          )),
      )

  def test_drop_interleaved_table(self):
    self._append_migrations(
        """
            class _Parent(spanner_orm.Model):
              __table__ = 'Parent'
              parent_key = spanner_orm.Field(
                  spanner_orm.String, primary_key=True)

            def upgrade():
              return spanner_orm.CreateTable(_Parent)
        """,
        """
            class _Parent(spanner_orm.Model):
              __table__ = 'Parent'
              parent_key = spanner_orm.Field(
                  spanner_orm.String, primary_key=True)

            class _Child(spanner_orm.Model):
              __table__ = 'Child'
              __interleaved__ = _Parent
              parent_key = spanner_orm.Field(
                  spanner_orm.String, primary_key=True)
              child_key = spanner_orm.Field(
                  spanner_orm.String, primary_key=True)

            def upgrade():
              return spanner_orm.CreateTable(_Child)
        """,
        """
            def upgrade():
              return spanner_orm.DropTable('Child')
        """,
    )
    self.run_orm_migrations(self._migrations_dir)
    self.assertCountEqual(
        ('Parent',),
        metadata.SpannerMetadata.tables().keys() - {'spanner_orm_migrations'},
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='does_not_exist',
          create_migrations=(),
          error_class=google_api_exceptions.NotFound,
      ),
      dict(
          testcase_name='has_secondary_index',
          create_migrations=(
              """
                  class _TableToDrop(spanner_orm.Model):
                    __table__ = 'TableToDrop'
                    key = spanner_orm.Field(
                        spanner_orm.String, primary_key=True)
                    value = spanner_orm.Field(spanner_orm.String)

                  def upgrade():
                    return spanner_orm.CreateTable(_TableToDrop)
              """,
              """
                  def upgrade():
                    return spanner_orm.CreateIndex(
                        table_name='TableToDrop',
                        index_name='value_index',
                        columns=['value'],
                    )
              """,
          ),
          error_class=google_api_exceptions.FailedPrecondition,
      ),
      dict(
          testcase_name='has_interleaved_child',
          create_migrations=(
              """
                  class _TableToDrop(spanner_orm.Model):
                    __table__ = 'TableToDrop'
                    parent_key = spanner_orm.Field(
                        spanner_orm.String, primary_key=True)

                  def upgrade():
                    return spanner_orm.CreateTable(_TableToDrop)
              """,
              """
                  class _TableToDrop(spanner_orm.Model):
                    __table__ = 'TableToDrop'
                    parent_key = spanner_orm.Field(
                        spanner_orm.String, primary_key=True)

                  class _Child(spanner_orm.Model):
                    __table__ = 'Child'
                    __interleaved__ = _TableToDrop
                    parent_key = spanner_orm.Field(
                        spanner_orm.String, primary_key=True)
                    child_key = spanner_orm.Field(
                        spanner_orm.String, primary_key=True)

                  def upgrade():
                    return spanner_orm.CreateTable(_Child)
              """,
          ),
          error_class=google_api_exceptions.FailedPrecondition,
      ),
  )
  def test_drop_table_error(
      self,
      *,
      create_migrations: Iterable[str],
      error_class: Type[Exception],
  ):
    self._append_migrations(*create_migrations)
    self.run_orm_migrations(self._migrations_dir)
    self._append_migrations("""
        def upgrade():
          return spanner_orm.DropTable('TableToDrop')
    """)
    with self.assertRaises(error_class):
      self.run_orm_migrations(self._migrations_dir)


if __name__ == '__main__':
  logging.basicConfig()
  absltest.main()

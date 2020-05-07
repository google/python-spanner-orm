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
"""Entry point for spanner_orm scripts."""

import argparse
from typing import Any

from spanner_orm import api
from spanner_orm.admin import migration_executor
from spanner_orm.admin import migration_manager


def generate(args: Any) -> None:
  manager = migration_manager.MigrationManager(args.directory)
  manager.generate(args.name)


def migrate(args: Any) -> None:
  connection = api.SpannerConnection(args.instance, args.database)
  executor = migration_executor.MigrationExecutor(connection, args.directory)
  executor.migrate(args.name)


def show_migrations(args: Any) -> None:
  connection = api.SpannerConnection(args.instance, args.database)
  executor = migration_executor.MigrationExecutor(connection, args.directory)
  executor.show_migrations()


def rollback(args: Any) -> None:
  connection = api.SpannerConnection(args.instance, args.database)
  executor = migration_executor.MigrationExecutor(connection, args.directory)
  executor.rollback(args.name)


def main(as_module: bool = False) -> None:
  prog = 'spanner-orm' if as_module else None
  parser = argparse.ArgumentParser(prog=prog)
  # 'subcommand' is actually required, but required subparsers are not supported
  # for Python < 3.7.
  subparsers = parser.add_subparsers(
      dest='subcommand',
      title='subcommands',
      description='valid subcommands')

  generate_parser = subparsers.add_parser(
      'generate', help='Generate a new migration')
  generate_parser.add_argument('name', help='Short name of the migration')
  generate_parser.add_argument('--directory')
  generate_parser.set_defaults(execute=generate)

  migrate_parser = subparsers.add_parser(
    'migrate', help='Execute unmigrated migrations')
  migrate_parser.add_argument(
    '--name', help='Stop migrating after this migration')
  migrate_parser.add_argument('--directory')
  migrate_parser.add_argument('instance', help='Name of Spanner instance')
  migrate_parser.add_argument('database', help='Name of Spanner database')
  migrate_parser.set_defaults(execute=migrate)

  show_migrations_parser = subparsers.add_parser(
    'showmigrations', help='List migrations')
  show_migrations_parser.add_argument('--directory')
  show_migrations_parser.add_argument('instance', help='Name of Spanner instance')
  show_migrations_parser.add_argument('database', help='Name of Spanner database')
  show_migrations_parser.set_defaults(execute=show_migrations)

  rollback_parser = subparsers.add_parser(
      'rollback', help='Roll back migrated migrations')
  rollback_parser.add_argument(
      'name', help='Keep rolling back past this migration')
  rollback_parser.add_argument('--directory')
  rollback_parser.add_argument('instance', help='Name of Spanner instance')
  rollback_parser.add_argument('database', help='Name of Spanner database')
  rollback_parser.set_defaults(execute=rollback)

  args = parser.parse_args()
  if args.subcommand is None:
    parser.print_help()
  else:
    args.execute(args)


if __name__ == '__main__':
  main(as_module=True)

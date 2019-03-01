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
"""Entry point for spanner_orm scripts."""
import argparse
from spanner_orm.admin import migration_manager


def generate(args):
  manager = migration_manager.MigrationManager(args.directory)
  manager.generate(args.name)


def main(as_module=False):
  prog = 'spanner-orm' if as_module else None
  parser = argparse.ArgumentParser(prog=prog)
  subparsers = parser.add_subparsers(
      title='subcommands', description='valid subcommands')

  generate_parser = subparsers.add_parser(
      'generate', help='Generate a new migration')
  generate_parser.add_argument('name', help='Short name of the migration')
  generate_parser.add_argument('--directory')
  generate_parser.set_defaults(execute=generate)

  args = parser.parse_args()
  if hasattr(args, 'execute'):
    args.execute(args)
  else:
    parser.print_help()


if __name__ == '__main__':
  main(as_module=True)

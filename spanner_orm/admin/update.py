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
"""Used with SpannerAdminApi to manage Spanner schema updates."""

import abc
from typing import Iterable, List, Optional, Type

from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import foreign_key_relationship
from spanner_orm import index
from spanner_orm import model
from spanner_orm.admin import api
from spanner_orm.admin import index_column
from spanner_orm.admin import metadata


class MigrationUpdate(abc.ABC):
  """Base class for all updates that can happen in a migration."""

  @abc.abstractmethod
  def execute(self) -> None:
    """Executes the update."""
    raise NotImplementedError


class NoUpdate(MigrationUpdate):
  """Update that does nothing, for migrations that don't update db schemas."""

  def execute(self) -> None:
    """See base class."""


class SchemaUpdate(MigrationUpdate, abc.ABC):
  """Base class for specifying schema updates."""

  @abc.abstractmethod
  def ddl(self) -> str:
    raise NotImplementedError

  def execute(self) -> None:
    self.validate()
    api.spanner_admin_api().update_schema(self.ddl())

  def validate(self) -> None:
    pass  # TODO(dseomn): Remove this method.


class CreateTable(SchemaUpdate):
  """Update that allows creating a new table."""

  def __init__(self, model_: Type[model.Model]):
    self._model = model_

  def ddl(self) -> str:
    key_fields = [
        '{} {}'.format(name, field.ddl())
        for name, field in self._model.fields.items()
    ]
    key_fields_ddl = ', '.join(key_fields)
    for relation in self._model.foreign_key_relations.values():
      key_fields_ddl += f', {relation.ddl}'
    index_ddl = 'PRIMARY KEY ({})'.format(', '.join(self._model.primary_keys))
    statement = 'CREATE TABLE {} ({}) {}'.format(self._model.table,
                                                 key_fields_ddl, index_ddl)

    if self._model.interleaved:
      statement += ', INTERLEAVE IN PARENT {parent} ON DELETE CASCADE'.format(
          parent=self._model.interleaved.table)
    return statement

  def validate(self) -> None:
    if not self._model.table:
      raise error.SpannerError('New table has no name')

    existing_model = metadata.SpannerMetadata.model(self._model.table)
    if existing_model:
      raise error.SpannerError('Table {} already exists'.format(
          self._model.table))

    if self._model.interleaved:
      self._validate_parent()

    self._validate_primary_keys()

    if self._model.indexes.keys() - {index.Index.PRIMARY_INDEX}:
      raise error.SpannerError(
          'Secondary indexes cannot be created by CreateTable; use CreateIndex '
          'in a separate migration.')

  def _validate_parent(self) -> None:
    """Verifies that the parent table information is valid."""
    parent_primary_keys = self._model.interleaved.primary_keys
    primary_keys = self._model.primary_keys

    message = 'Table {} is not a child of parent table {}'.format(
        self._model.table, self._model.interleaved.table)
    for parent_key, key in zip(parent_primary_keys, primary_keys):
      if parent_key != key:
        raise error.SpannerError(message)
    if len(parent_primary_keys) > len(primary_keys):
      raise error.SpannerError(message)

  def _validate_primary_keys(self) -> None:
    """Verifies that the primary key data is valid."""
    if not self._model.primary_keys:
      raise error.SpannerError('Table {} has no primary key'.format(
          self._model.table))

    for key in self._model.primary_keys:
      if key not in self._model.fields:
        raise error.SpannerError(
            'Table {} column {} in primary key but not in schema'.format(
                self._model.table, key))


class DropTable(SchemaUpdate):
  """Update for dropping an existing table."""

  def __init__(self, table_name: str):
    self._table = table_name

  def ddl(self) -> str:
    return 'DROP TABLE {}'.format(self._table)


class AddColumn(SchemaUpdate):
  """Update for adding a column to an existing table.

  Only supports adding nullable columns
  """

  def __init__(self, table_name: str, column_name: str, field_: field.Field):
    self._table = table_name
    self._column = column_name
    self._field = field_

  def ddl(self) -> str:
    return 'ALTER TABLE {} ADD COLUMN {} {}'.format(self._table, self._column,
                                                    self._field.ddl())

  def validate(self) -> None:
    model_ = metadata.SpannerMetadata.model(self._table)
    if not model_:
      raise error.SpannerError('Table {} does not exist'.format(self._table))
    if not self._field.nullable():
      raise error.SpannerError('Column {} is not nullable'.format(self._column))
    if self._field.primary_key():
      raise error.SpannerError('Column {} is a primary key'.format(
          self._column))


class DropColumn(SchemaUpdate):
  """Update for dropping a column from an existing table."""

  def __init__(self, table_name: str, column_name: str):
    self._table = table_name
    self._column = column_name

  def ddl(self) -> str:
    return 'ALTER TABLE {} DROP COLUMN {}'.format(self._table, self._column)

  def validate(self) -> None:
    model_ = metadata.SpannerMetadata.model(self._table)
    if not model_:
      raise error.SpannerError('Table {} does not exist'.format(self._table))

    if self._column not in model_.fields:
      raise error.SpannerError('Column {} does not exist on {}'.format(
          self._column, self._table))

    # Verify no indices exist on the column we're trying to drop
    num_indexed_columns = index_column.IndexColumnSchema.count(
        condition.equal_to('column_name', self._column),
        condition.equal_to('table_name', self._table),
    )
    if num_indexed_columns > 0:
      raise error.SpannerError('Column {} is indexed'.format(self._column))


class AlterColumn(SchemaUpdate):
  """Update for altering a column an existing table.

  Only supports changing the nullability of a column
  """

  def __init__(self, table_name: str, column_name: str, field_: field.Field):
    self._table = table_name
    self._column = column_name
    self._field = field_

  def ddl(self) -> str:
    return 'ALTER TABLE {} ALTER COLUMN {} {}'.format(self._table, self._column,
                                                      self._field.ddl())

  def validate(self) -> None:
    model_ = metadata.SpannerMetadata.model(self._table)
    if not model_:
      raise error.SpannerError('Table {} does not exist'.format(self._table))

    if self._column not in model_.fields:
      raise error.SpannerError('Column {} does not exist on {}'.format(
          self._column, self._table))

    if self._column in model_.primary_keys:
      raise error.SpannerError('Column {} is a primary key on {}'.format(
          self._column, self._table))

    old_field = model_.fields[self._column]
    # Validate that the only alteration is to change column nullability
    if self._field.field_type().ddl() != old_field.field_type().ddl():
      raise error.SpannerError('Column {} is changing type'.format(
          self._column))
    if self._field.nullable() == old_field.nullable():
      raise error.SpannerError('Column {} has no changes'.format(self._column))


class CreateIndex(SchemaUpdate):
  """Update for creating an index on an existing table."""

  def __init__(self,
               table_name: str,
               index_name: str,
               columns: Iterable[str],
               interleaved: Optional[str] = None,
               null_filtered: bool = False,
               unique: bool = False,
               storing_columns: Optional[Iterable[str]] = None):
    self._table = table_name
    self._index = index_name
    self._columns = columns
    self._parent_table = interleaved
    self._null_filtered = null_filtered
    self._unique = unique
    self._storing_columns = storing_columns or []

  def ddl(self) -> str:
    statement = 'CREATE'
    if self._unique:
      statement += ' UNIQUE'
    if self._null_filtered:
      statement += ' NULL_FILTERED'
    statement += (f' INDEX {self._index} '
                  f'ON {self._table} ({", ".join(self._columns)})')
    if self._storing_columns:
      statement += 'STORING ({})'.format(', '.join(self._storing_columns))
    if self._parent_table:
      statement += ', INTERLEAVE IN {}'.format(self._parent_table)
    return statement

  def validate(self) -> None:
    model_ = metadata.SpannerMetadata.model(self._table)
    if not model_:
      raise error.SpannerError('Table {} does not exist'.format(self._table))

    if not self._columns:
      raise error.SpannerError('Index {} has no columns'.format(self._index))

    if self._index in model_.indexes:
      raise error.SpannerError('Index {} already exists'.format(self._index))

    self._validate_columns(model_)

    if self._parent_table:
      self._validate_parent(model_)

  def _validate_columns(self, model_: Type[model.Model]) -> None:
    """Verifies all columns exist and are not part of the primary key."""
    for column in self._columns:
      if column not in model_.columns:
        raise error.SpannerError('Table {} has no column {}'.format(
            self._table, column))

    for column in self._storing_columns:
      if column not in model_.columns:
        raise error.SpannerError('Table {} has no column {}'.format(
            self._table, column))
      if column in model_.primary_keys:
        raise error.SpannerError('{} is part of the primary key for {}'.format(
            column, self._table))

  def _validate_parent(self, model_: Type[model.Model]) -> None:
    """Verifies this index can be interleaved in the parent table."""
    parent = model_.interleaved
    while parent:
      if parent == self._parent_table:
        break
      parent = parent.interleaved

    if not parent:
      raise error.SpannerError('{} is not a parent of table {}'.format(
          self._parent_table, self._table))


class DropIndex(SchemaUpdate):
  """Update for dropping a secondary index on an existing table."""

  def __init__(self, table_name: str, index_name: str):
    self._table = table_name
    self._index = index_name

  def ddl(self) -> str:
    return 'DROP INDEX {}'.format(self._index)

  def validate(self) -> None:
    model_ = metadata.SpannerMetadata.model(self._table)
    if not model_:
      raise error.SpannerError('Table {} does not exist'.format(self._table))

    db_index = model_.indexes.get(self._index)
    if not db_index:
      raise error.SpannerError('Index {} does not exist'.format(self._index))
    if db_index.primary:
      raise error.SpannerError('Index {} is the primary index'.format(
          self._index))


class ExecutePartitionedDml(MigrationUpdate):
  """Update for running arbitrary partitioned DML.

  NOTE: Partitioned DML queries should be idempotent. See
  https://cloud.google.com/spanner/docs/dml-partitioned for details, and more
  information about partitioned DML.
  """

  def __init__(self, dml: str):
    self._dml = dml

  def execute(self) -> None:
    """See base class."""
    api.spanner_admin_api().execute_partitioned_dml(self._dml)


def model_creation_ddl(model_: Type[model.Model]) -> List[str]:
  """Returns the list of ddl statements needed to create the model's table."""
  ddl_list = [CreateTable(model_).ddl()]

  for model_index in model_.indexes.values():
    if model_index.primary:
      continue
    create_index = CreateIndex(
        model_.table,
        model_index.name,
        model_index.columns,
        interleaved=model_index.parent,
        storing_columns=model_index.storing_columns)
    ddl_list.append(create_index.ddl())

  return ddl_list

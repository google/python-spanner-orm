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
"""Used with SpannerAdminApi to manage Spanner schema updates."""

import abc

from spanner_orm import condition
from spanner_orm.admin import api
from spanner_orm.admin import index_column
from spanner_orm.admin import metadata


class SchemaUpdate(abc.ABC):
  """Base class for specifying schema updates."""

  @abc.abstractmethod
  def ddl(self):
    raise NotImplementedError

  def execute(self):
    self.validate()
    api.SpannerAdminApi.update_schema(self.ddl())

  @abc.abstractmethod
  def validate(self):
    raise NotImplementedError


class CreateTableUpdate(SchemaUpdate):
  """Update that allows creating a new table."""

  def __init__(self, model):
    self._model = model
    self._existing_model = metadata.SpannerMetadata.model(model.table)

  def ddl(self):
    return self._model.creation_ddl

  def _validate_parent(self):
    parent_primary_keys = self._model.interleaved.primary_keys
    primary_keys = self._model.primary_keys, ('Non-matching primary keys in '
                                              'interleaved table')
    assert len(parent_primary_keys) <= len(primary_keys)
    for parent_key, key in zip(parent_primary_keys, primary_keys):
      assert parent_key == key, 'Non-matching primary keys in interleaved table'

  def _validate_primary_keys(self):
    assert self._model.primary_keys, 'Creating a table with no primary key'
    for key in self._model.primary_keys:
      assert key in self._model.schema, 'Trying to index fields not in table'

  def validate(self):
    assert self._model.table, 'Trying to create a table with no name'
    assert not self._existing_model, ('Trying to create a table that already '
                                      'exists')
    if self._model.interleaved:
      self._validate_parent()
    self._validate_primary_keys()


class ColumnUpdate(SchemaUpdate):
  """Specifies column updates such as ADD, DROP, and ALTER."""

  def __init__(self, table_name, column_name, field):
    self._table = table_name
    self._column = column_name
    self._field = field
    self._model = metadata.SpannerMetadata.model(table_name)

  def ddl(self):
    if self._field is None:
      return 'ALTER TABLE {} DROP COLUMN {}'.format(self._table, self._column)
    elif self._column in self._model.schema:
      operation = 'ALTER COLUMN'
    else:
      operation = 'ADD COLUMN'
    return 'ALTER TABLE {} {} {} {}'.format(self._table, operation,
                                            self._column, self._field.ddl())

  def _validate_alter_column(self):
    assert self._column in self._model.schema, 'Altering a nonexistent column'
    old_field = self._model.schema[self._column]
    # Validate that the only alteration is to change column nullability
    assert self._field.field_type() == old_field.field_type(
    ), 'Changing the type of a column'
    assert self._field.nullable() != old_field.nullable()

  def _validate_drop_column(self):
    assert self._column in self._model.schema, 'Dropping a nonexistent column'
    # Verify no indices exist on the column we're trying to drop
    num_index_columns = index_column.IndexColumnSchema.count(
        None, condition.equal_to('column_name', self._column),
        condition.equal_to('table_name', self._table))
    assert num_index_columns == 0, 'Dropping an indexed column'

  def validate(self):
    assert self._model
    if self._field is None:
      self._validate_drop_column()
    elif self._column in self._model.schema:
      self._validate_alter_column()
    else:
      assert self._field.nullable(), 'Adding a non-nullable column'


class IndexUpdate(SchemaUpdate):
  """Specifies index updates such as ADD and DROP."""

  def __init__(self, table_name, index_name, columns):
    self._table = table_name
    self._index = index_name
    self._columns = columns
    self._model = metadata.SpannerMetadata.model(table_name)

  # TODO(dbrandao): implement
  def ddl(self):
    raise NotImplementedError

  # TODO(dbrandao): implement
  def validate(self):
    assert self._model
    raise NotImplementedError


class NoUpdate(SchemaUpdate):
  """Update that does nothing, for migrations that don't update db schemas."""
  def ddl(self):
    return ''

  def execute(self):
    pass

  def validate(self):
    pass

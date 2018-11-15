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
from spanner_orm import schemas


class SchemaUpdate(abc.ABC):
  """Base class for specifying schema updates."""

  @abc.abstractmethod
  def ddl(self):
    pass

  @abc.abstractmethod
  def table(self):
    pass

  @abc.abstractmethod
  def validate(self, schema):
    pass


class ColumnUpdate(SchemaUpdate):
  """Specifies column updates such as ADD, DROP, and ALTER."""

  def __init__(self, table_name, column_name, field):
    self._table = table_name
    self._column = column_name
    self._field = field

  def ddl(self, model):
    if self._field is None:
      return 'ALTER TABLE {} DROP COLUMN {}'.format(self._table, self._column)
    elif self._column in model.schema:
      operation = 'ALTER COLUMN'
    else:
      operation = 'ADD COLUMN'
    return 'ALTER TABLE {} {} {} {}'.format(self._table, operation,
                                            self._column, self._field.ddl())

  def table(self):
    return self._table

  def _validate_alter_column(self, model):
    assert self._column in model.schema
    old_field = model.schema[self._column]
    # Validate that the only alteration is to change column nullability
    assert self._field.field_type() == old_field.field_type()
    assert self._field.nullable() != old_field.nullable()

  def _validate_drop_column(self, model):
    assert self._column in model.schema
    # Verify no indices exist on the column we're trying to drop
    num_index_columns = schemas.IndexColumnSchema.count(
        None, condition.EqualityCondition('column_name', self._column),
        condition.EqualityCondition('table_name', self._table))
    assert num_index_columns == 0

  def validate(self, model):
    if self._field is None:
      self._validate_drop_column(model)
    elif self._column in model.schema:
      self._validate_alter_column(model)
    else:
      assert self._field.nullable()


class IndexUpdate(SchemaUpdate):
  """Specifies index updates such as ADD and DROP."""

  def __init__(self, table_name, index_name, columns):
    self._table = table_name
    self._index = index_name
    self._columns = columns

  # TODO(dbrandao): implement
  def ddl(self, model):
    raise NotImplementedError

  def table(self):
    return self._table

  # TODO(dbrandao): implement
  def validate(self, model):
    raise NotImplementedError


class CreateTableUpdate(SchemaUpdate):
  """Update that allows creating a new table."""

  def __init__(self, table_name, schema):
    self._table = table_name
    self._schema = schema

  # TODO(dbrandao): implement
  def ddl(self):
    raise NotImplementedError

  def table(self):
    return self._table

  # TODO(dbrandao): implement
  def validate(self):
    raise NotImplementedError

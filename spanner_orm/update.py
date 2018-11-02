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
"""Used with DatabaseAdminApi to manage database schema updates."""

from abc import ABC
from abc import abstractmethod

from spanner_orm.condition import EqualityCondition
from spanner_orm.schemas.index_column import IndexColumnSchema
from spanner_orm.type import NullableType


class SchemaUpdate(ABC):
  """Base class for specifying database schema update."""

  @abstractmethod
  def ddl(self):
    pass

  @abstractmethod
  def table(self):
    pass

  @abstractmethod
  def validate(self, schema):
    pass


class ColumnUpdate(SchemaUpdate):
  """Specifies column updates such as ADD, DROP, and ALTER."""

  def __init__(self, table_name, column_name, db_type):
    self._table = table_name
    self._column = column_name
    self._type = db_type

  def ddl(self, model):
    if self._type is None:
      return 'ALTER TABLE {} DROP COLUMN {}'.format(self._table, self._column)
    elif self._column in model.schema():
      operation = 'ALTER COLUMN'
    else:
      operation = 'ADD COLUMN'
    return 'ALTER TABLE {} {} {} {}'.format(self._table, operation,
                                            self._column, self._type.full_ddl())

  def table(self):
    return self._table

  def _validate_alter_column(self, model):
    """Validates that the only alteration is to change column nullability."""
    # Verify type is actually changing
    assert self._column in model.schema()
    old_type = model.schema()[self._column]
    assert self._type != old_type
    assert old_type.db_type() == self._type.db_type()

    if issubclass(old_type, NullableType):
      # Type was nullable, so must now be the not nullable type
      assert not issubclass(self._type, NullableType)
    else:
      # Type wasn't nullable, so it must now be nullable
      assert issubclass(self._type, NullableType)

  def _validate_drop_column(self, model):
    assert self._column in model.schema()
    # Verify no indices exist on the column we're trying to drop
    num_index_columns = IndexColumnSchema.count(
        None, EqualityCondition('column_name', self._column),
        EqualityCondition('table_name', self._table))
    assert num_index_columns == 0

  def validate(self, model):
    if self._type is None:
      self._validate_drop_column(model)
    elif self._column in model.schema():
      self._validate_alter_column(model)
    else:
      assert issubclass(self._type, NullableType)


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

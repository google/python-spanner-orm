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
"""Retrieves table metadata from Spanner."""

import collections

from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import model
from spanner_orm import schemas
from spanner_orm import update
from spanner_orm.admin import api


class SpannerMetadata(object):
  """Gathers information about a table from Spanner."""

  @classmethod
  def column_update(cls, schema_change):
    """Handles an ALTER TABLE schema update."""
    if not isinstance(schema_change, update.ColumnUpdate):
      raise error.SpannerError('column_update must be provided a ColumnUpdate')
    klass = cls.models()[schema_change.table()]
    schema_change.validate(klass)

    api.SpannerAdminApi.update_schema(schema_change.ddl(klass))

  @classmethod
  def create_table(cls, schema_change):
    """Handles a CREATE TABLE schema update."""
    if not isinstance(schema_change, update.CreateTableUpdate):
      raise error.SpannerError(
          'create_table must be provided a CreateTableUpdate')
    all_models = cls.models()
    if schema_change.table() in all_models:
      raise error.SpannerError('Cannot create a table that already exists')
    schema_change.validate()

    api.SpannerAdminApi.update_schema(schema_change.ddl())

  @classmethod
  def index_update(cls, schema_change):
    """Handles a CREATE INDEX or DROP INDEX schema update."""
    if not isinstance(schema_change, update.IndexUpdate):
      raise error.SpannerError('index_update must be provided a IndexUpdate')
    klass = cls.models()[schema_change.table()]
    schema_change.validate(klass)

    api.SpannerAdminApi.update_schema(schema_change.ddl(klass))

  @classmethod
  def models(cls, transaction=None):
    """Constructs model classes from Spanner table schema."""
    tables = cls._tables(transaction)
    indexes = cls._indexes(transaction)
    results = {}

    for table_name, fields in tables.items():
      primary_index = indexes[table_name]['PRIMARY_KEY']['columns']
      klass = model.ModelBase('Model_{}'.format(table_name), (model.Model,), {})
      for f in fields.values():
        if f.name in primary_index:
          f._primary_key = True  # pylint: disable=protected-access
      klass.meta = model.Metadata(table=table_name, fields=fields)
      results[table_name] = klass
    return results

  @classmethod
  def _tables(cls, transaction=None):
    """Compiles table information from column schema."""
    tables = collections.defaultdict(dict)
    columns = schemas.ColumnSchema.where(
        transaction, condition.equal_to('table_catalog', ''),
        condition.equal_to('table_schema', ''))
    for column in columns:
      new_field = field.Field(column.field_type(), nullable=column.nullable())
      new_field.name = column.column_name
      new_field.index = column.ordinal_position
      tables[column.table_name][column.column_name] = new_field
    return tables

  @classmethod
  def _indexes(cls, transaction=None):
    """Compiles index information from index and index columns schemas."""
    # ordinal_position is the position of the column in the indicated index.
    # Results are ordered by that so the index columns are added in the correct
    # order. None indicates that the key isn't really a part of the index, so we
    # skip those
    index_column_schemas = schemas.IndexColumnSchema.where(
        transaction, condition.equal_to('table_catalog', ''),
        condition.equal_to('table_schema', ''),
        condition.not_equal_to('ordinal_position', None),
        condition.order_by(('ordinal_position', condition.OrderType.ASC)))

    index_columns = collections.defaultdict(list)
    for schema in index_column_schemas:
      key = (schema.table_name, schema.index_name)
      index_columns[key].append(schema.column_name)

    index_schemas = schemas.IndexSchema.where(
        transaction, condition.equal_to('table_catalog', ''),
        condition.equal_to('table_schema', ''))
    indexes = collections.defaultdict(dict)
    for schema in index_schemas:
      indexes[schema.table_name][schema.index_name] = {
          'columns': index_columns[(schema.table_name, schema.index_name)],
          'type': schema.index_type,
          'unique': schema.is_unique,
          'state': schema.index_state
      }

    return indexes

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
from spanner_orm import field
from spanner_orm import model
from spanner_orm.admin import column
from spanner_orm.admin import index
from spanner_orm.admin import index_column


class SpannerMetadata(object):
  """Gathers information about a table from Spanner."""
  _models = None
  _tables = None
  _indexes = None

  @classmethod
  def models(cls):
    """Constructs model classes from Spanner table schema."""
    if cls._models is None:
      tables = cls.tables()
      indexes = cls.indexes()
      results = {}

      for table_name, fields in tables.items():
        primary_index = indexes[table_name]['PRIMARY_KEY']['columns']
        klass = model.ModelBase('Model_{}'.format(table_name), (model.Model,),
                                {})
        for f in fields.values():
          if f.name in primary_index:
            f._primary_key = True  # pylint: disable=protected-access
        klass.meta = model.Metadata(table=table_name, fields=fields)
        results[table_name] = klass
      cls._models = results
    return cls._models

  @classmethod
  def model(cls, table_name):
    return cls.models().get(table_name)

  @classmethod
  def tables(cls):
    """Compiles table information from column schema."""
    if cls._tables is None:
      tables = collections.defaultdict(dict)
      columns = column.ColumnSchema.where(
          None, condition.equal_to('table_catalog', ''),
          condition.equal_to('table_schema', ''))
      for column_row in columns:
        new_field = field.Field(
            column_row.field_type(), nullable=column_row.nullable())
        new_field.name = column_row.column_name
        new_field.index = column_row.ordinal_position
        tables[column_row.table_name][column_row.column_name] = new_field
      cls._tables = tables
    return cls._tables

  @classmethod
  def indexes(cls):
    """Compiles index information from index and index columns schemas."""
    if cls._indexes is None:
      # ordinal_position is the position of the column in the indicated index.
      # Results are ordered by that so the index columns are added in the
      # correct order. None indicates that the key isn't really a part of the
      # index, so we skip those
      index_column_schemas = index_column.IndexColumnSchema.where(
          None, condition.equal_to('table_catalog', ''),
          condition.equal_to('table_schema', ''),
          condition.not_equal_to('ordinal_position', None),
          condition.order_by(('ordinal_position', condition.OrderType.ASC)))

      index_columns = collections.defaultdict(list)
      for schema in index_column_schemas:
        key = (schema.table_name, schema.index_name)
        index_columns[key].append(schema.column_name)

      index_schemas = index.IndexSchema.where(
          None, condition.equal_to('table_catalog', ''),
          condition.equal_to('table_schema', ''))
      indexes = collections.defaultdict(dict)
      for schema in index_schemas:
        indexes[schema.table_name][schema.index_name] = {
            'columns': index_columns[(schema.table_name, schema.index_name)],
            'type': schema.index_type,
            'unique': schema.is_unique,
            'state': schema.index_state
        }
      cls._indexes = indexes

    return cls._indexes

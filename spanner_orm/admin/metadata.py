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
"""Retrieves table metadata from Spanner."""

import collections
from typing import Any, Dict, Optional, Type

from spanner_orm import condition
from spanner_orm import field
from spanner_orm import index
from spanner_orm import metadata
from spanner_orm import model
from spanner_orm.admin import column
from spanner_orm.admin import index as index_schema
from spanner_orm.admin import index_column
from spanner_orm.admin import table


class SpannerMetadata(object):
  """Gathers information about a table from Spanner."""

  @classmethod
  def _class_name_from_table(cls, table_name: Optional[str]) -> Optional[str]:
    if table_name:
      return 'table_{}_model'.format(table_name)
    return None

  @classmethod
  def models(cls) -> Dict[str, Type[model.Model]]:
    """Constructs model classes from Spanner table schema."""
    tables = cls.tables()
    indexes = cls.indexes()
    models = {}

    for table_name, table_data in tables.items():
      primary_index = indexes[table_name][index.Index.PRIMARY_INDEX]
      primary_keys = set(primary_index.columns)
      klass = model.ModelMetaclass(
          cls._class_name_from_table(table_name), (model.Model,), {})
      for model_field in table_data['fields'].values():
        model_field._primary_key = model_field.name in primary_keys  # pylint: disable=protected-access

      klass.meta = metadata.ModelMetadata(
          table=table_name,
          fields=table_data['fields'],
          interleaved=cls._class_name_from_table(table_data['parent_table']),
          indexes=indexes[table_name],
          model_class=klass)
      klass.meta.finalize()
      models[table_name] = klass

    return models

  @classmethod
  def model(cls, table_name) -> Optional[Type[model.Model]]:
    return cls.models().get(table_name)

  @classmethod
  def tables(cls) -> Dict[str, Dict[str, Any]]:
    """Compiles table information from column schema."""
    column_data = collections.defaultdict(dict)
    columns = column.ColumnSchema.where(None,
                                        condition.equal_to('table_catalog', ''),
                                        condition.equal_to('table_schema', ''))
    for column_row in columns:
      new_field = field.Field(
          column_row.field_type, nullable=column_row.nullable)
      new_field.name = column_row.column_name
      new_field.position = column_row.ordinal_position
      column_data[column_row.table_name][column_row.column_name] = new_field

    table_data = collections.defaultdict(dict)
    tables = table.TableSchema.where(None,
                                     condition.equal_to('table_catalog', ''),
                                     condition.equal_to('table_schema', ''))
    for table_row in tables:
      name = table_row.table_name
      table_data[name]['parent_table'] = table_row.parent_table_name
      table_data[name]['fields'] = column_data[name]
    return table_data

  @classmethod
  def indexes(cls) -> Dict[str, Dict[str, Any]]:
    """Compiles index information from index and index columns schemas."""
    # ordinal_position is the position of the column in the indicated index.
    # Results are ordered by that so the index columns are added in the
    # correct order.
    index_column_schemas = index_column.IndexColumnSchema.where(
        None, condition.equal_to('table_catalog', ''),
        condition.equal_to('table_schema', ''),
        condition.order_by(('ordinal_position', condition.OrderType.ASC)))

    index_columns = collections.defaultdict(list)
    storing_columns = collections.defaultdict(list)
    for schema in index_column_schemas:
      key = (schema.table_name, schema.index_name)
      if schema.ordinal_position is not None:
        index_columns[key].append(schema.column_name)
      else:
        storing_columns[key].append(schema.column_name)

    index_schemas = index_schema.IndexSchema.where(
        None, condition.equal_to('table_catalog', ''),
        condition.equal_to('table_schema', ''))
    indexes = collections.defaultdict(dict)
    for schema in index_schemas:
      key = (schema.table_name, schema.index_name)
      new_index = index.Index(
          index_columns[key],
          parent=schema.parent_table_name,
          null_filtered=schema.is_null_filtered,
          unique=schema.is_unique,
          storing_columns=storing_columns[key])
      new_index.name = schema.index_name
      indexes[schema.table_name][schema.index_name] = new_index
    return indexes
